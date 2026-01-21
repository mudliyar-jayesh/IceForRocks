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
}
