using System.Runtime.Intrinsics.Arm;

namespace IceForRocks.Core.Ingestion;

public unsafe class Igloo : IDisposable
{

    private readonly FileStream _stream;
    private readonly BinaryWriter _writer;
    private ulong _nextTxId;
    
    // "ICEW" in hex
    private const uint Magic = 0x49434557;

    public Igloo(string path, ulong startingTxId)
    {
        _stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read, 4096,
            FileOptions.WriteThrough);
        _writer = new BinaryWriter(_stream);
        _nextTxId = startingTxId;
    }

    public ulong Log(ReadOnlySpan<byte> payload)
    {
        long startPosition = _stream.Position;
        ulong currentTxId = _nextTxId++;
        _writer.Write(Magic);
        _writer.Write(currentTxId);
        _writer.Write(payload.Length);
        
        uint checksum = ComputeChecksum(payload);
        _writer.Write(checksum);
        
        _writer.Write(payload);
        _stream.Flush(true);
        return currentTxId;
    }

    private static uint ComputeChecksum(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        for (int i = 0; i < data.Length; i++)
        {
            byte b = data[i];
            crc ^= b;
            for (int j = 0; j < 8; j++)
            {
                uint mask = (uint)-(int)(crc & 1); // googled this. TODO: study on this.
                crc = (crc >> 1) ^ (0xEDB88320 & mask);
            }
        }

        return ~crc;
    }
    
    public void Dispose()
    {
        _writer.Dispose();
        _stream.Dispose();
    }
}