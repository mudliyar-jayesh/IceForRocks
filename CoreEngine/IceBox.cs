namespace IceForRocks.Core;

public class IceBox<T> : IDisposable
    where T : unmanaged
{
    private readonly string _basePath;
    private IceFreezer<T> _freezer;

    private readonly ReaderWriterLockSlim _fileLock;

    public IceBox(string dbPath)
    {
        _basePath = dbPath;

        _fileLock = IceVault.GetLock(_basePath);
    }

    public void Dispose() { }

    public void Write(List<T> records, Func<T, ulong> maskGenerator, bool append)
    {
        if (_freezer != null)
        {
            _freezer.Dispose();
        }
        _freezer = new IceFreezer<T>(_basePath, maskGenerator, append);
        _fileLock.EnterWriteLock();
        foreach (T record in records)
        {
            _freezer.Append(record);
        }
        _freezer.FlushDataAndIdx();
        _freezer.Dispose();
        _fileLock.ExitWriteLock();
    }

    public List<T> Search(IceQuery<T> query)
    {
        _fileLock.EnterReadLock();

        try
        {
            var breaker = new IceBreaker<T>(_basePath);
            return breaker.Search(query);
        }
        finally
        {
            _fileLock.ExitReadLock();
        }
    }

    public List<TResult> Select<TResult>(IceQuery<T> query, IceSelector<T, TResult> selector)
    {
        _fileLock.EnterReadLock();

        try
        {
            var breaker = new IceBreaker<T>(_basePath);
            return breaker.Select(query, selector);
        }
        finally
        {
            _fileLock.ExitReadLock();
        }
    }

    public void Update(IceQuery<T> query, RefAction<T> updateAction)
    {
        _fileLock.EnterWriteLock();

        try
        {
            var breaker = new IceBreaker<T>(_basePath);
            breaker.ApplyUpdate(query, updateAction);
        }
        finally
        {
            _fileLock.ExitWriteLock();
        }
    }
}
