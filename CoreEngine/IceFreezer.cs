using System.Runtime.InteropServices;

namespace IceForRocks;

public class IceFreezer<T> : IDisposable
    where T : unmanaged
{
    private readonly FileStream _stream;
    private readonly BinaryWriter _writer;
    private readonly List<SegmentHeader> _headers = new();
    private readonly T[] _segmentBuffer;
    private readonly int _recordSize;
    private int _bufferIndex = 0;
    private readonly Func<T, ulong> _maskGenerator;
    private const int SegmentSize = 4096;

    public IceFreezer(string filePath, Func<T, ulong> maskGenerator)
    {
        _recordSize = Marshal.SizeOf<T>();
        _maskGenerator = maskGenerator;
        _segmentBuffer = new T[SegmentSize];

        _stream = new FileStream(
            filePath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            1024 * 1024
        );
        _writer = new BinaryWriter(_stream);
    }

    public void Append(T record)
    {
        _segmentBuffer[_bufferIndex++] = record;
        if (_bufferIndex >= SegmentSize)
        {
            FlushSegment();
        }
    }

    private void FlushSegment()
    {
        ulong mask = 0;
        for (int i = 0; i < _bufferIndex; i++)
        {
            mask |= _maskGenerator(_segmentBuffer[i]);
        }
        _headers.Add(new SegmentHeader() { Bitmask = mask });

        var span = _segmentBuffer.AsSpan(0, _bufferIndex);
        var bytes = MemoryMarshal.AsBytes(span);
        _stream.Write(bytes);

        _bufferIndex = 0;
    }

    public void Close()
    {
        if (_bufferIndex > 0)
        {
            FlushSegment();
        }

        var headerSpan = CollectionsMarshal.AsSpan(_headers);
        _stream.Write(MemoryMarshal.AsBytes(headerSpan));

        _writer.Write(_headers.Count);
        _writer.Flush();
    }

    public void Dispose()
    {
        _writer.Dispose();
        _stream.Dispose();
    }
}
