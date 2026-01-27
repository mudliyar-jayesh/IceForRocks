using System.Runtime.InteropServices;
using IceForRocks.Ingestion;

namespace IceForRocks.Core;

public class IceFreezer<T> : IDisposable
    where T : unmanaged
{
    private readonly FileStream _dataStream;
    private readonly FileStream _idxStream;

    private readonly BinaryWriter _dataWriter;
    private readonly BinaryWriter _idxWriter;

    private int _dataBufferIndex = 0;

    private readonly Func<T, ulong> _maskGenerator;

    private const int SegmentCapacity = 4096;

    private readonly T[] _segmentBuffer;

    public IceFreezer(string filePath, Func<T, ulong> maskGenerator, bool append)
    {
        _segmentBuffer = new T[SegmentCapacity];
        _maskGenerator = maskGenerator;

        var dataDirectory = Path.GetDirectoryName(filePath)!;
        if (!Directory.Exists(dataDirectory))
        {
            Directory.CreateDirectory(dataDirectory);
        }

        var (fileName, extension) = FileParser<T>.GetFileNameAndExtension(filePath);

        string idxFilePath = Path.Combine(dataDirectory, $"{fileName}_idx{extension}");

        FileMode mode = append ? FileMode.Append : FileMode.Create;

        _idxStream = new FileStream(idxFilePath, mode, FileAccess.Write, FileShare.ReadWrite, 1024);

        _dataStream = new FileStream(
            filePath,
            mode,
            FileAccess.Write,
            FileShare.ReadWrite,
            1024 * 1024
        );

        _dataWriter = new BinaryWriter(_dataStream);
        _idxWriter = new BinaryWriter(_idxStream);
    }

    public void Append(T record)
    {
        _segmentBuffer[_dataBufferIndex++] = record;
        if (_dataBufferIndex >= SegmentCapacity)
        {
            FlushDataAndIdx();
        }
    }

    public void FlushDataAndIdx()
    {
        var headers = new SegmentHeader[_dataBufferIndex];
        for (int i = 0; i < _dataBufferIndex; i++)
        {
            ulong dataMask = 0;
            dataMask |= _maskGenerator(_segmentBuffer[i]);
            headers[i] = new SegmentHeader { Bitmask = dataMask };
        }

        var headerSpan = headers.AsSpan(0, headers.Length);
        var headerBytes = MemoryMarshal.AsBytes(headerSpan);
        _idxWriter.Write(headerBytes);

        var dataSpan = _segmentBuffer.AsSpan(0, _dataBufferIndex);
        var dataBytes = MemoryMarshal.AsBytes(dataSpan);
        _dataStream.Write(dataBytes);

        _dataBufferIndex = 0; // reset
    }

    public void Complete()
    {
        if (_dataBufferIndex > 0)
        {
            FlushDataAndIdx();
        }

        _idxWriter.Flush();
        _dataWriter.Flush();
    }

    public void Dispose()
    {
        _idxWriter.Dispose();
        _idxStream.Dispose();
        _dataWriter.Dispose();
        _dataStream.Dispose();
    }
}
