using System.ComponentModel.DataAnnotations;
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
                var writeMethod = typeof(BinaryHelper).GetMethod(
                    "WriteString",
                    new[] { typeof(string), typeof(IntPtr), typeof(int) }
                );

                var val = Expression.Property(src, prop);
                block.Add(Expression.Call(writeMethod, val, fieldPtr, Expression.Constant(size)));
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
                var writeMethod = typeof(BinaryHelper).GetMethod("WriteBytes");

                // Equivalent to: BinaryHelper.WriteBytes(src.PasswordHash, fieldPtr, 64);
                block.Add(
                    Expression.Call(
                        writeMethod,
                        Expression.Property(src, prop),
                        fieldPtr,
                        Expression.Constant(size)
                    )
                );
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                var toStoreMethod = typeof(DateHelper).GetMethod("ToStoreFormat");
                var dateString = Expression.Call(toStoreMethod, Expression.Property(src, prop));

                var writeMethod = typeof(BinaryHelper).GetMethod(
                    "WriteString",
                    new[] { typeof(string), typeof(IntPtr), typeof(int) }
                );

                // Pass '8' as the limit. WriteString must handle the full 8 bytes!
                block.Add(
                    Expression.Call(writeMethod, dateString, fieldPtr, Expression.Constant(8))
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

                var readMethod = typeof(BinaryHelper).GetMethod(
                    "ReadString",
                    new[] { typeof(IntPtr), typeof(int) }
                );

                var poolMethod = typeof(StringPool).GetMethod("GetOrAdd");

                var readCall = Expression.Call(readMethod, fieldPtr, Expression.Constant(size));
                var pooledCall = Expression.Call(poolMethod, readCall);

                block.Add(Expression.Assign(Expression.Property(instance, prop), pooledCall));
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
            else if (prop.PropertyType == typeof(byte[]))
            {
                var size = prop.GetCustomAttribute<BinarySizeAttribute>()?.Size ?? 64;
                var readMethod = typeof(BinaryHelper).GetMethod("ReadBytes");

                // FIX: You MUST initialize the array in the class first
                // Equivalent to: instance.PasswordHash = new byte[64];
                var newArray = Expression.NewArrayBounds(typeof(byte), Expression.Constant(size));
                var assignArray = Expression.Assign(Expression.Property(instance, prop), newArray);

                // Equivalent to: BinaryHelper.ReadBytes(fieldPtr, instance.PasswordHash, 64);
                var callRead = Expression.Call(
                    readMethod,
                    fieldPtr,
                    Expression.Property(instance, prop),
                    Expression.Constant(size)
                );

                block.Add(assignArray);
                block.Add(callRead);
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                var readMethod = typeof(BinaryHelper).GetMethod(
                    "ReadString",
                    new[] { typeof(IntPtr), typeof(int) }
                );

                // Dates are fixed 8-byte yyyyMMdd strings
                var rawStringCall = Expression.Call(readMethod, fieldPtr, Expression.Constant(8));

                // Cleanup: Remove nulls and spaces
                var trimMethod = typeof(string).GetMethod("Trim", new[] { typeof(char[]) });
                var trimChars = Expression.Constant(new char[] { '\0', ' ' });
                var cleanStringCall = Expression.Call(rawStringCall, trimMethod, trimChars);

                // Parse back to DateTime object
                var parseMethod = typeof(DateHelper).GetMethod("ParseSafe");
                var dateTimeResult = Expression.Call(parseMethod, cleanStringCall);

                block.Add(Expression.Assign(Expression.Property(instance, prop), dateTimeResult));
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
