using System.Runtime.InteropServices;

namespace IceForRocks.Core.Metadata;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct DryIceHeader
{
    public ulong Magic;
    public uint Version;
    public long RowCount;
    public uint ColumnCount;
    public ulong LastTxId;
    public long CreatedAt;
}

public class DryIce
{
    private readonly List<IcicleDefinition> _column = new();

    public void AddColumn(string name, Type type, int offset)
    {
        _column.Add(new IcicleDefinition()
        {
            Name = name,
            DataType = type.Name,
            Offset = offset,
        });
    }

    public class IcicleDefinition
    {
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public int Offset { get; set; }
    }
}
