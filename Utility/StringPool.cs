using System.Collections.Concurrent;

namespace IceForRocks;

public static class StringPool
{
    // this is basically a dumb way to not create a new string object
    private static readonly ConcurrentDictionary<string, string> _pool = new();

    public static string GetOrAdd(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }
        return _pool.GetOrAdd(value, value);
    }
}
