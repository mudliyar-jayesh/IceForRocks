using System.Collections.Concurrent;

namespace IceForRocks.Core;

public static class IceVault
{
    private static readonly ConcurrentDictionary<string, ReaderWriterLockSlim> _lock = new();

    public static ReaderWriterLockSlim GetLock(string filePath)
    {
        string key = Path.GetFullPath(filePath).ToLowerInvariant();

        return _lock.GetOrAdd(
            key,
            _ => new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion)
        );
    }
}
