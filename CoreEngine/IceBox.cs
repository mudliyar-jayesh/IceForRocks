namespace IceForRocks.Core;

public class IceBox<T>
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

    public void Update(IceQuery<T> query, Action<T> updateAction)
    {
        _fileLock.EnterReadLock();

        try
        {
            RefAction<T> action = delegate(ref T item)
            {
                updateAction(item);
            };
            _breaker.ApplyUpdate(query, action);
        }
        finally
        {
            _fileLock.ExitReadLock();
        }
    }
}
