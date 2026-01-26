namespace IceForRocks.Core;

public class IceBox<T> : IDisposable
    where T : unmanaged
{
    private readonly string _basePath;
    private readonly IceBreaker<T> _breaker;

    private readonly ReaderWriterLockSlim _fileLock;

    public IceBox(string dbPath)
    {
        _basePath = dbPath;

        _fileLock = IceVault.GetLock(_basePath);
        _breaker = new IceBreaker<T>(_basePath);
    }

    public void Dispose() { }

    // TODO: handle pagination later
    public List<T> Search(IceQuery<T> query)
    {
        _fileLock.EnterReadLock();

        try
        {
            return _breaker.Search(query);
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
            return _breaker.Select(query, selector);
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
            _breaker.ApplyUpdate(query, updateAction);
        }
        finally
        {
            _fileLock.ExitWriteLock();
        }
    }
}
