using System.ComponentModel;
using System.Text;

namespace IceForRocks.CoreV2;

public class IceHeap : IDisposable
{
    private FileStream? _stream;
    public string? FilePath { get; }


    public IceHeap(string path)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
        FilePath = path;
        _stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
        _stream.Seek(0, SeekOrigin.End);
    }

    public (long Offset, int Length) Write(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return (0, 0);
        }
        long start = _stream!.Position;
        byte[] data = Encoding.UTF8.GetBytes(value);
        _stream.Write(data);
        return (start, data.Length);
    }

    public string Read(long offset, int length)
    {
        if (length == 0)
        {
            return string.Empty;
        }
        // if existing write is in process. not sure, have to test this. (// TODO)
        using var stream = new FileStream(FilePath!, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        stream.Seek(offset, SeekOrigin.Begin);
        byte[] data = new byte[length];
        stream.Read(data,0, length);
        return Encoding.UTF8.GetString(data);
    }

    public void Commit()
    {
        _stream?.Flush();
    }

    public void Dispose()
    {
        _stream!.Dispose();
    }
}