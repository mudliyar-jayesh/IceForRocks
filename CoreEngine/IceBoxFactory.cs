using System.Collections.Concurrent;

namespace IceForRocks.Core;

public interface IIceBoxFactory
{
    IceBox<T> Create<T>(string dbPath)
        where T : unmanaged;

    public IceBox<T> GetDatabase<T>(string dbPath)
        where T : unmanaged;
    public void SwapDatabase<T>(string dbPath)
        where T : unmanaged;
}

public class IceBoxFactory : IIceBoxFactory, IDisposable
{
    private readonly ConcurrentDictionary<string, IDisposable> _activeConnections = new();
    private readonly object _swapLock = new();

    public IceBox<T> GetDatabase<T>(string dbPath)
        where T : unmanaged
    {
        lock (_swapLock)
        {
            if (_activeConnections.TryGetValue(dbPath, out var existingDb))
            {
                return (IceBox<T>)existingDb;
            }

            var newDb = new IceBox<T>(dbPath);
            _activeConnections.TryAdd(dbPath, newDb);
            return newDb;
        }
    }

    public void SwapDatabase<T>(string dbPath)
        where T : unmanaged
    {
        string? directory = Path.GetDirectoryName(dbPath);
        if (!Directory.Exists(directory))
        {
            throw new Exception("Could not swap database, directory does not exist");
        }
        string? fileName = Path.GetFileNameWithoutExtension(dbPath);
        string idxPath = Path.Combine(directory!, $"{fileName}_idx.bin");
        string tempDbPath = $"{dbPath}.tmp";
        string tempIdxPath = $"{idxPath}.tmp";

        lock (_swapLock)
        {
            if (_activeConnections.TryRemove(dbPath, out var currentDb))
            {
                currentDb.Dispose();
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (File.Exists(dbPath))
            {
                File.Delete(dbPath);
            }
            if (File.Exists(idxPath))
            {
                File.Delete(idxPath);
            }

            File.Move(tempDbPath, dbPath);
            File.Move(tempIdxPath, idxPath);

            var newDb = new IceBox<T>(dbPath);
            _activeConnections.TryAdd(dbPath, newDb);
        }
    }

    public IceBox<T> Create<T>(string dbPath)
        where T : unmanaged
    {
        return new IceBox<T>(dbPath);
    }

    public void Dispose()
    {
        foreach (var db in _activeConnections.Values)
        {
            _activeConnections.Clear();
        }
    }
}
