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

    public static void WriteString(string source, IntPtr dest, int limit)
    {
        if (string.IsNullOrEmpty(source))
            return;
        var bytes = System.Text.Encoding.UTF8.GetBytes(source);
        int length = Math.Min(bytes.Length, limit - 1);
        Marshal.Copy(bytes, 0, dest, length);
        Marshal.WriteByte(dest, length, 0); // Null terminator
    }

    public static string ReadString(IntPtr src, int limit)
    {
        int length = 0;
        while (length < limit && Marshal.ReadByte(src, length) != 0)
            length++;
        byte[] buffer = new byte[length];
        Marshal.Copy(src, buffer, 0, length);
        return System.Text.Encoding.UTF8.GetString(buffer);
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
        if (source == null)
            return;
        Marshal.Copy(source, 0, dest, Math.Min(source.Length, limit));
    }
}
