using System.Runtime.CompilerServices;

namespace IceForRocks.Core.Storage;

public unsafe class Icicle<T> where T : unmanaged
{
    private readonly IceSheet _sheet;
    private readonly int _elementSize;

    public Icicle(IceSheet sheet)
    {
        _sheet = sheet;
        _elementSize = sizeof(T);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T Peek(long rowIndex)
    {
        byte* ptr = _sheet.GetHandle(rowIndex * _elementSize);
        return *(T*)ptr;
    }

    public void Freeze(long rowIndex, T value)
    {
        byte* ptr = _sheet.GetHandle(rowIndex * _elementSize);
        if (_elementSize == 8)
        {
            Interlocked.Exchange(ref *(long*)ptr, *(long*)&value);
        } 
        else if (_elementSize == 4)
        {
            Interlocked.Exchange(ref *(int*)ptr, *(int*)&value);
        }
        else
        {
            *(T*)ptr = value;
        }
    }
}