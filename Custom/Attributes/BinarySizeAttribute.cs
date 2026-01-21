using System.ComponentModel.Design.Serialization;

namespace IceForRocks;

[AttributeUsage(AttributeTargets.Property)]
public class BinarySizeAttribute : Attribute
{
    public int Size { get; }

    public BinarySizeAttribute(int size) => Size = size;
}
