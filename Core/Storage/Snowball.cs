using System.Runtime.CompilerServices;

namespace IceForRocks.Core.Storage;

public unsafe class Snowball
{
    private readonly IceSheet _sheet;

    private readonly long _capacityInBits;
    
    public long CapacityInBits => _capacityInBits;

    public Snowball(IceSheet sheet)
    {
        _sheet = sheet;
        _capacityInBits = _sheet.Capacity * 8;
    }

    public void Make(long index)
    {
        var (offset, position) = GetOffsetAndPosition(index);
        ulong *ptr =(ulong*)_sheet.GetHandle(offset);
        ulong mask = 1UL << position;
        Interlocked.Or(ref *(long*)ptr, (long)mask);
    }

    public void Break(long index)
    {
        var (offset, position) = GetOffsetAndPosition(index);
        ulong *ptr =(ulong*)_sheet.GetHandle(offset);
        ulong mask = ~(1UL << position);
        Interlocked.And(ref *(long*)ptr, (long)mask);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsActive(long index)
    {
        var (offset, position) = GetOffsetAndPosition(index);
        ulong *ptr =(ulong*)_sheet.GetHandle(offset);
        return (*ptr & (1UL << position)) != 0;
    }

    private (long offset, int position) GetOffsetAndPosition(long index) => ((index >> 6) << 3, (int)index & 63);
}