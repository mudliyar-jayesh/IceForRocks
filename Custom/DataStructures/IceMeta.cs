// basically to get offsets and sizes without in loop reflection for every record.
using System.Reflection;
using System.Runtime.InteropServices;
using IceForRocks;

public static class IceMetaStruct<TStruct>
    where TStruct : unmanaged
{
    private static readonly Dictionary<string, int> _sizes = new();
    private static readonly Dictionary<string, int> _offsets = new();

    static IceMetaStruct()
    {
        foreach (var field in typeof(TStruct).GetFields())
        {
            var sizeAttribute = field.GetCustomAttribute<BinarySizeAttribute>();
            if (sizeAttribute != null)
            {
                _sizes[field.Name] = sizeAttribute.Size;
            }

            _offsets[field.Name] = (int)Marshal.OffsetOf<TStruct>(field.Name);
        }
    }

    // TODO: size is not there for record structs, add them later for better use
    public static int Size(string fieldName) => _sizes[fieldName];

    public static int Offset(string fieldName) => _offsets[fieldName];
}
