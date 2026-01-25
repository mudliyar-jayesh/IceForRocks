using System.Runtime.InteropServices;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct SegmentHeader
{
    public ulong Bitmask; // basically, tags for each record
}
