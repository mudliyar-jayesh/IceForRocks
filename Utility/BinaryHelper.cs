using System.Runtime.InteropServices;

namespace IceForRocks;

public static class BinaryHelper
{
    public static unsafe void ToFixedBytes(string source, byte* target, int limit)
    {
        if (string.IsNullOrEmpty(source))
            return;

        var bytes = System.Text.Encoding.UTF8.GetBytes(source);
        int length = Math.Min(bytes.Length, limit - 1);

        for (int i = 0; i < length; i++)
        {
            target[i] = bytes[i];
        }
        target[length] = 0; // acts as null terminator
    }

    public static unsafe string FromFixedBytes(byte* target, int limit)
    {
        int length = 0;
        while (length < limit && target[length] != 0)
        {
            length++;
        }
        return System.Text.Encoding.UTF8.GetString(target, length);
    }

    public static void WriteStringOld(string source, IntPtr dest, int limit)
    {
        //  Zeroing out the memory first to prevent leftover data from old records
        //  padding to data
        for (int i = 0; i < limit; i++)
            Marshal.WriteByte(dest, i, 0);

        if (string.IsNullOrEmpty(source))
            return;

        var bytes = System.Text.Encoding.UTF8.GetBytes(source);

        // writing full 8 bytes. if limit is 8 bytes. had issues with datetime to string storing. so this is a hack
        int length = Math.Min(bytes.Length, limit);
        Marshal.Copy(bytes, 0, dest, length);
    }

    public static unsafe void WriteString(string source, IntPtr destPtr, int limit)
    {
        byte* dest = (byte*)destPtr;

        // Clear the target memory
        for (int i = 0; i < limit; i++)
            dest[i] = 0;

        if (string.IsNullOrEmpty(source))
            return;

        var bytes = System.Text.Encoding.UTF8.GetBytes(source);

        // Write exactly what fits (No -1, No manual null terminator)
        int length = Math.Min(bytes.Length, limit);
        Marshal.Copy(bytes, 0, destPtr, length);
    }

    public static string ReadStringOld(IntPtr src, int limit)
    {
        byte[] buffer = new byte[limit];
        Marshal.Copy(src, buffer, 0, limit);

        // Get the string and trim the trailing nulls (zeros) and spaces
        return System.Text.Encoding.UTF8.GetString(buffer).TrimEnd('\0', ' ');
    }

    public static unsafe string ReadString(byte* ptr, int maxLength)
    {
        if (*ptr == 0)
        {
            return string.Empty;
        }

        int length = 0;
        while (length < maxLength && ptr[length] != 0)
        {
            length++;
        }
        return System.Text.Encoding.UTF8.GetString(ptr, length);
    }

    public static unsafe string ReadString(IntPtr srcPtr, int limit)
    {
        byte* src = (byte*)srcPtr;
        if (src == null)
            return string.Empty;

        // Find the actual length of the string (up to the first null or the limit)
        int actualLength = 0;
        while (actualLength < limit && src[actualLength] != 0)
        {
            actualLength++;
        }

        // Only convert the bytes that actually contain data
        return System.Text.Encoding.UTF8.GetString(src, actualLength);
    }

    public static void WriteDecimal(IntPtr dest, decimal value)
    {
        int[] bits = decimal.GetBits(value);
        Marshal.Copy(bits, 0, dest, 4); // Decimals are 4 ints (16 bytes)
    }

    public static decimal ReadDecimal(IntPtr src)
    {
        int[] bits = new int[4];
        Marshal.Copy(src, bits, 0, 4);
        return new decimal(bits);
    }

    public static void WriteBytes(byte[] source, IntPtr dest, int limit)
    {
        if (dest == IntPtr.Zero)
            return;

        unsafe
        {
            // Clear the target memory first
            NativeMemory.Clear((void*)dest, (nuint)limit);
        }

        if (source == null || source.Length == 0)
            return;

        // Copy the bytes
        int count = Math.Min(source.Length, limit);
        Marshal.Copy(source, 0, dest, count);
    }

    public static void ReadBytes(IntPtr source, byte[] dest, int limit)
    {
        if (source == IntPtr.Zero || dest == null)
            return;

        // Copy from fixed buffer in struct to the managed array
        Marshal.Copy(source, dest, 0, Math.Min(dest.Length, limit));
    }
}
