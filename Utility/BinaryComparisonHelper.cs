using System.Reflection;
using System.Reflection.Metadata;

public static unsafe class BinaryComparisonHelper
{
    public static bool CompareString(IntPtr ptr, int limit, string target)
    {
        if (target == null)
        {
            return false;
        }

        byte* src = (byte*)ptr;
        if (src[0] == 0)
            return target.Length == 0;

        ReadOnlySpan<byte> sourceSpan = new ReadOnlySpan<byte>(src, limit);
        int nullIdx = sourceSpan.IndexOf((byte)0);
        int valueLength = nullIdx == -1 ? limit : nullIdx;

        Span<byte> targetSpan = stackalloc byte[target.Length * 3]; // max value of UTF8
        int targetLength = System.Text.Encoding.UTF8.GetBytes(target, targetSpan);
        return sourceSpan.Slice(0, valueLength).SequenceEqual(targetSpan.Slice(0, targetLength));
    }

    public static bool CompareDate(IntPtr ptr, string target)
    {
        // note that target format should be yyyyMMdd
        byte* src = (byte*)ptr;
        for (int i = 0; i < 8; i++)
        {
            if (src[i] != target[i])
            {
                return false;
            }
        }
        return true;
    }

    // as decimals are stored as 16-byte structs, and in c# the are in 4 ints, we can directly compare bits
    public static bool CompareDecimal(IntPtr ptr, decimal target)
    {
        int* src = (int*)ptr;
        int* tar = (int*)&target;
        bool match = true;
        for (int i = 0; i < 4; i++)
        {
            if (src[i] != tar[i])
            {
                match = false;
            }
        }
        return match;
    }

    public static bool CompareDecimalRange(IntPtr ptr, decimal min, decimal max)
    {
        decimal val = *(decimal*)ptr;
        return val >= min && val <= max;
    }

    public static bool CompareDateRange(IntPtr ptr, string minDate, string maxDate)
    {
        // dates are stored in yyyyMMdd format, so sequenctial comparison works. as its alphabetical
        byte* src = (byte*)ptr;
        ReadOnlySpan<byte> dateSpan = new ReadOnlySpan<byte>(src, 8);

        if (string.Compare(System.Text.Encoding.UTF8.GetString(src, 8), minDate) < 0)
        {
            return false;
        }

        if (string.Compare(System.Text.Encoding.UTF8.GetString(src, 8), maxDate) > 0)
        {
            return false;
        }
        return true;
    }
}
