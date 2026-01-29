using System.Collections.Concurrent;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace IceForRocks.CoreV2;

public unsafe class IceStore<TRow> : IDisposable where TRow : unmanaged
{
    public IceFile<TRow> _block { get; }
    public IceHeap _slush { get; }

    private readonly string _mapPath;
    private readonly Dictionary<string, IceMap> _maps = new();

    private readonly ReaderWriterLockSlim _lock = new ();

    public IceStore(string rootPath, string tableName)
    {
        var tableDirectory = Path.Combine(rootPath, $".{tableName}");
        Directory.CreateDirectory(tableDirectory);

        _block = new IceFile<TRow>(Path.Combine(tableDirectory, "data.block"));
        _slush = new IceHeap(Path.Combine(tableDirectory, "data.slush"));

        _mapPath = Path.Combine(tableDirectory, "maps");
        Directory.CreateDirectory(_mapPath);
    }

    public int GetId(string mapName, string value)
    {
        return GetMap(mapName).GetOrAdd(value);
    }
    public string GetValue(string mapName, int id)
    {
        return GetMap(mapName).GetValue(id);
    }

    public void Insert(TRow row)
    {
        _lock.EnterWriteLock();
        try
        {
            _block.Append(row);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public (long Offset, int Length) WriteSlush(string text)
    {
        return _slush.Write(text);
    }

    public string ReadSlush(long offset, int length)
    {
        return _slush.Read(offset, length);
    }

    private List<int> FindMatchingIndices(Func<TRow, bool> predicate, int? skip = null, int? take = null)
    {
        List<int> matchingIndices = new();
        _lock.EnterReadLock();
        try
        {
            unsafe
            {
                TRow* ptr = _block.BasePointer;
                int count = _block.Count;
                for (int i = 0; i < count; i++)
                {
                    if (predicate(ptr[i]))
                    {
                        matchingIndices.Add(i);
                    }
                }
            }
            if (skip is null || take is null)
            {
                return matchingIndices;
            }
            return matchingIndices.Skip(skip.Value).Take(take.Value).ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public IEnumerable<TRow> Scan(Func<TRow, bool> predicate)
    {
        var indices = FindMatchingIndices(predicate);
        foreach (var index in indices)
        {
            yield return GetRowAt(index);
        }
    }

    public IEnumerable<TResult> Select<TResult>(Func<TRow, bool> predicate, Func<TRow, IceStore<TRow>, TResult> selector)
    {
        var indices = FindMatchingIndices(predicate);

        foreach (var index in indices)
        {
            yield return selector(GetRowAt(index), this);

        }

    }

    public double Sum(Func<TRow, bool> predicate, Func<TRow, double> valueSelector)
    {
        _lock.EnterReadLock();
        try
        {
            var ptr = _block.BasePointer;
            var count = _block.Count;
            double total = 0;
            object lockObj = new object();
            Parallel.ForEach(Partitioner.Create(0, count), range =>
            {
                double localSum = 0;
                for (int i =  range.Item1; i < range.Item2; i++)
                {
                    if (predicate(ptr[i]))
                    {
                        localSum += valueSelector(ptr[i]);
                    }
                }
                lock (lockObj)
                {
                    total += localSum;
                }
            });
            return total;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public IceMap GetMap(string name)
    {
        if (_maps.ContainsKey(name))
        {
            return _maps[name];
        }

        var path = Path.Combine(_mapPath, $"{name}.map");
        var map = new IceMap(path);
        _maps[name] = map;

        return map;
    }

    private TRow GetRowAt(int index)
    {
        _lock.EnterReadLock();
        try
        {
            unsafe
            {
                return _block.BasePointer[index];
            }
        } 
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void Dispose()
    {
        _block.Dispose();
        _slush.Dispose();
        foreach (var map in _maps.Values)
        {
            map.Dispose();
        }
        _lock.Dispose();
    }
}