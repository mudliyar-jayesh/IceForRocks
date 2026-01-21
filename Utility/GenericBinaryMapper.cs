using System.Reflection;
using System.Runtime.InteropServices;

namespace IceForRocks;

// TODO: This generic implementation will remain slow, have to replace this with expression trees.
public static class GenericBinaryMapper
{
    public static unsafe void MapToStruct<TClass, TStruct>(TClass source, ref TStruct target)
        where TStruct : unmanaged
    {
        fixed (TStruct* ptr = &target)
        {
            byte* basePtr = (byte*)ptr;
            var properties = typeof(TClass).GetProperties();
            var fields = typeof(TStruct).GetFields();

            foreach (var prop in properties)
            {
                var field = typeof(TStruct).GetField(prop.Name);
                if (field == null)
                    continue;

                var value = prop.GetValue(source);
                if (value == null)
                    continue;

                int offset = (int)Marshal.OffsetOf<TStruct>(field.Name);
                if (value is string s)
                {
                    var attr = prop.GetCustomAttribute<BinarySizeAttribute>();
                    int size = attr?.Size ?? 32;
                    WriteString(s, basePtr + offset, size);
                }
                else if (value is byte[] bytes)
                {
                    var attr = prop.GetCustomAttribute<BinarySizeAttribute>();
                    int size = attr?.Size ?? bytes.Length;
                    Marshal.Copy(
                        bytes,
                        0,
                        (IntPtr)(basePtr + offset),
                        Math.Min(bytes.Length, size)
                    );
                }
                else
                {
                    field.SetValueDirect(__makeref(target), value);
                }
            }
        }
    }

    private static unsafe void WriteString(string s, byte* dest, int max)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(s);
        int length = Math.Min(bytes.Length, max - 1);
        Marshal.Copy(bytes, 0, (IntPtr)dest, length);
        dest[length] = 0; // null terminator
    }
}
