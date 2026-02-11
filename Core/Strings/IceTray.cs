using System.Runtime.CompilerServices;
using IceForRocks.Core.Storage;

namespace  IceForRocks.Core.Strings;

public unsafe class IceTray
{
    private readonly IceSheet _heap;
    private readonly Icicle<long> _offsets;
    private readonly Snowball _bloomFilter;

    private long _heapTail = 0;
    private int _nextId = 0;

    public IceTray(IceSheet heap, Icicle<long> offsets, Snowball bloomFilter)
    {
        _heap = heap;
        _offsets = offsets;
        _bloomFilter = bloomFilter; 
    }

    public static uint GetHash(ReadOnlySpan<byte> data)
    {
        uint hash = 2166136261;
        foreach (byte b in data)
        {
            hash = (hash ^ b) * 167777619;
        }

        return hash;
    }

    public int GetOrAdd(ReadOnlySpan<byte> data)
    {
        uint hash = GetHash(data);
        if (_bloomFilter.IsActive(hash % _bloomFilter.CapacityInBits))
        {
            return AddToTray(data, hash);
        }

        for (int i = 0; i < _nextId; i++)
        {
            if (Compare(i, data))
            {
                return i;
            }
        }

        return AddToTray(data, hash);
    }

    private int AddToTray(ReadOnlySpan<byte> data, uint hash)
    {
        int id = _nextId++;
        _offsets.Freeze(id, _heapTail);
        byte* dest = _heap.GetHandle(_heapTail);
        fixed (byte* src = data)
        {
            Unsafe.CopyBlock(dest, src, (uint)data.Length);
        }
        _heapTail += data.Length;
        _bloomFilter.Make(hash % _bloomFilter.CapacityInBits);

        return id;
    }

    private bool Compare(int id, ReadOnlySpan<byte> data)
    {
        long start = _offsets.Peek(id);
        long nextStart = (id + 1 < _nextId) ? _offsets.Peek(id + 1) : _heapTail;
        int len = (int)(nextStart - start);

        if (len != data.Length) return false;
        ReadOnlySpan<byte> existing = new ReadOnlySpan<byte>(_heap.GetHandle(start), len);
        return data.SequenceEqual(existing);
    }
}