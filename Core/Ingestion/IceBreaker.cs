using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using IceForRocks.Core.Storage;
using IceForRocks.Core.Strings;

namespace IceForRocks.Core.Ingestion;

public unsafe class IceBreaker
{

    private readonly List<IShredAction> _actions = new();

    public void AddIcicle<T>(int offset, Icicle<T> column) where T : unmanaged
    {
        _actions.Add(new NumericAction<T>(offset, column));
    }

    public void AddSnowball(int offset, Snowball snowball)
    {
        _actions.Add(new BitAction(offset, snowball));
    }

    public void AddIceTray(int offset, IceTray tray, Icicle<int> idColumn)
    {
        _actions.Add(new StringAction(offset, tray, idColumn));
    }

    public void Break(long rowIndex, ReadOnlySpan<byte> row)
    {
        var actionSpan = CollectionsMarshal.AsSpan(_actions);
        for (int i = 0; i < actionSpan.Length; i++)
        {
            actionSpan[i].Execute(rowIndex, row);
        }
    }
}

#region Optimized Internal Actions

internal interface IShredAction
{
    void Execute(long rowIndex, ReadOnlySpan<byte> payload);
}

internal class NumericAction<T> : IShredAction where T : unmanaged
{
    private readonly int _offset;
    private readonly Icicle<T> _column;

    public NumericAction(int offset, Icicle<T> column)
    {
        _offset = offset; 
        _column = column;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Execute(long rowIndex, ReadOnlySpan<byte> row)
    {
        T value = Unsafe.ReadUnaligned<T>(ref MemoryMarshal.GetReference(row.Slice(_offset)));
        _column.Freeze(rowIndex, value);
    }
}

internal class BitAction : IShredAction
{
    private readonly int _offset;
    private readonly Snowball _snowball;

    public BitAction(int offset, Snowball snowball)
    {
        _offset = offset;
        _snowball = snowball;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Execute(long rowIndex, ReadOnlySpan<byte> row)
    {
        if (row[_offset] != 0)
        {
            _snowball.Make(rowIndex);
            return;
        }

        _snowball.Break(rowIndex);
    }
}


internal class StringAction : IShredAction
{
    private readonly int _offset;
    private readonly IceTray _tray;
    private readonly Icicle<int> _idColumn;

    public StringAction(int offset, IceTray tray, Icicle<int> idColumn)
    {
        _offset = offset;
        _tray = tray;
        _idColumn = idColumn;
    }

    public void Execute(long rowIndex, ReadOnlySpan<byte> row)
    {
        ushort length = Unsafe.ReadUnaligned<ushort>(ref MemoryMarshal.GetReference(row.Slice(_offset)));

        ReadOnlySpan<byte> stringData = row.Slice(_offset + 2, length);
        int id = _tray.GetOrAdd(stringData);
        _idColumn.Freeze(rowIndex, id);
    }
}


#endregion