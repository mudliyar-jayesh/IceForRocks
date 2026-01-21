using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;

namespace IceForRocks;

public static class FastMapper<TClass, TStruct>
    where TStruct : unmanaged
    where TClass : class, new()
{
    private static readonly Action<TClass, IntPtr> _toStruct;
    private static readonly Func<IntPtr, TClass> _toClass;

    static FastMapper()
    {
        _toStruct = CompileToStruct();
        _toClass = CompileToClass();
    }

    public static unsafe void MapToStruct(TClass source, ref TStruct target)
    {
        fixed (TStruct* ptr = &target)
            _toStruct(source, (IntPtr)ptr);
    }

    public static unsafe TClass MapToClass(ref TStruct source)
    {
        fixed (TStruct* ptr = &source)
            return _toClass((IntPtr)ptr);
    }

    private static Action<TClass, IntPtr> CompileToStruct()
    {
        var src = Expression.Parameter(typeof(TClass), "src");
        var destPtr = Expression.Parameter(typeof(IntPtr), "destPtr");
        var block = new List<Expression>();

        foreach (var prop in typeof(TClass).GetProperties())
        {
            var field = typeof(TStruct).GetField(prop.Name);
            if (field == null)
                continue;

            var offset = Marshal.OffsetOf<TStruct>(field.Name);

            // FIX: Convert IntPtr to long, add offset, convert back to IntPtr
            var fieldPtr = Expression.Convert(
                Expression.Add(
                    Expression.Convert(destPtr, typeof(long)),
                    Expression.Constant((long)offset)
                ),
                typeof(IntPtr)
            );

            if (prop.PropertyType == typeof(string))
            {
                var size = prop.GetCustomAttribute<BinarySizeAttribute>()?.Size ?? 32;
                var method = typeof(BinaryHelper).GetMethod(
                    "WriteString",
                    BindingFlags.Public | BindingFlags.Static
                );
                block.Add(
                    Expression.Call(
                        method,
                        Expression.Property(src, prop),
                        fieldPtr,
                        Expression.Constant(size)
                    )
                );
            }
            else if (prop.PropertyType == typeof(bool))
            {
                var byteVal = Expression.Condition(
                    Expression.Property(src, prop),
                    Expression.Constant((byte)1),
                    Expression.Constant((byte)0)
                );
                block.Add(
                    Expression.Call(
                        typeof(Marshal).GetMethod(
                            "WriteByte",
                            new[] { typeof(IntPtr), typeof(byte) }
                        ),
                        fieldPtr,
                        byteVal
                    )
                );
            }
            else if (prop.PropertyType == typeof(byte[]))
            {
                var size = prop.GetCustomAttribute<BinarySizeAttribute>()?.Size ?? 64;
                var method = typeof(BinaryHelper).GetMethod(
                    "WriteBytes",
                    BindingFlags.Public | BindingFlags.Static
                );
                block.Add(
                    Expression.Call(
                        method,
                        Expression.Property(src, prop),
                        fieldPtr,
                        Expression.Constant(size)
                    )
                );
            }
            else // Handle Primitives (int, long, decimal)
            {
                Type propType = prop.PropertyType;
                MethodInfo writeMethod = null;

                if (propType == typeof(long))
                    writeMethod = typeof(Marshal).GetMethod(
                        "WriteInt64",
                        new[] { typeof(IntPtr), typeof(long) }
                    );
                else if (propType == typeof(int))
                    writeMethod = typeof(Marshal).GetMethod(
                        "WriteInt32",
                        new[] { typeof(IntPtr), typeof(int) }
                    );
                else if (propType == typeof(short))
                    writeMethod = typeof(Marshal).GetMethod(
                        "WriteInt16",
                        new[] { typeof(IntPtr), typeof(short) }
                    );
                else if (propType == typeof(byte))
                    writeMethod = typeof(Marshal).GetMethod(
                        "WriteByte",
                        new[] { typeof(IntPtr), typeof(byte) }
                    );
                else if (propType == typeof(decimal))
                {
                    // Decimal is special; we use our own helper to write it as raw bytes
                    writeMethod = typeof(BinaryHelper).GetMethod(
                        "WriteDecimal",
                        BindingFlags.Public | BindingFlags.Static
                    );
                }

                if (writeMethod != null)
                {
                    block.Add(
                        Expression.Call(writeMethod, fieldPtr, Expression.Property(src, prop))
                    );
                }
            }
        }

        return Expression
            .Lambda<Action<TClass, IntPtr>>(Expression.Block(block), src, destPtr)
            .Compile();
    }

    private static Func<IntPtr, TClass> CompileToClass()
    {
        var srcPtr = Expression.Parameter(typeof(IntPtr), "srcPtr");
        var instance = Expression.Variable(typeof(TClass), "obj");
        var block = new List<Expression>
        {
            Expression.Assign(instance, Expression.New(typeof(TClass))),
        };

        foreach (var prop in typeof(TClass).GetProperties())
        {
            var field = typeof(TStruct).GetField(prop.Name);
            if (field == null)
                continue;

            var offset = Marshal.OffsetOf<TStruct>(field.Name);

            // FIX: Convert IntPtr to long, add offset, convert back to IntPtr
            var fieldPtr = Expression.Convert(
                Expression.Add(
                    Expression.Convert(srcPtr, typeof(long)),
                    Expression.Constant((long)offset)
                ),
                typeof(IntPtr)
            );

            if (prop.PropertyType == typeof(string))
            {
                var size = prop.GetCustomAttribute<BinarySizeAttribute>()?.Size ?? 32;
                var method = typeof(BinaryHelper).GetMethod(
                    "ReadString",
                    BindingFlags.Public | BindingFlags.Static
                );
                block.Add(
                    Expression.Assign(
                        Expression.Property(instance, prop),
                        Expression.Call(method, fieldPtr, Expression.Constant(size))
                    )
                );
            }
            else if (prop.PropertyType == typeof(bool))
            {
                var val = Expression.Call(
                    typeof(Marshal).GetMethod("ReadByte", new[] { typeof(IntPtr) }),
                    fieldPtr
                );
                block.Add(
                    Expression.Assign(
                        Expression.Property(instance, prop),
                        Expression.Equal(val, Expression.Constant((byte)1))
                    )
                );
            }
            else
            {
                Type propType = prop.PropertyType;
                MethodInfo readMethod = null;

                if (propType == typeof(long))
                    readMethod = typeof(Marshal).GetMethod("ReadInt64", new[] { typeof(IntPtr) });
                else if (propType == typeof(int))
                    readMethod = typeof(Marshal).GetMethod("ReadInt32", new[] { typeof(IntPtr) });
                else if (propType == typeof(short))
                    readMethod = typeof(Marshal).GetMethod("ReadInt16", new[] { typeof(IntPtr) });
                else if (propType == typeof(byte))
                    readMethod = typeof(Marshal).GetMethod("ReadByte", new[] { typeof(IntPtr) });
                else if (propType == typeof(decimal))
                {
                    readMethod = typeof(BinaryHelper).GetMethod(
                        "ReadDecimal",
                        BindingFlags.Public | BindingFlags.Static
                    );
                }

                if (readMethod != null)
                {
                    block.Add(
                        Expression.Assign(
                            Expression.Property(instance, prop),
                            Expression.Call(readMethod, fieldPtr)
                        )
                    );
                }
            }
        }

        block.Add(instance);
        return Expression
            .Lambda<Func<IntPtr, TClass>>(Expression.Block(new[] { instance }, block), srcPtr)
            .Compile();
    }
}
