using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

public static unsafe class SIMDSearch
{
    /*
    compare 32bytes, basically faster then Sequnceequal or string.Equals
    first, load 32 bytes from pointer to YMM register
    then compare all 32 bytes at the same time
    move the result to a bitmask. if all 32 bytes do match,
    then the mask must be 0xFFFFFFFF (all bits set). this is easier to compare
    */
    public static bool Equals32(byte* source, Vector256<byte> targetVector)
    {
        Vector256<byte> data = Avx2.LoadVector256(source);

        Vector256<byte> result = Avx2.CompareEqual(data, targetVector);

        return (uint)Avx2.MoveMask(result) == 0xFFFFFFFF;
    }

    // convert string to 32 byte vector for SIMD
    public static Vector256<byte> CreateTargetVector(string match)
    {
        byte[] bytes = new byte[32];
        System.Text.Encoding.UTF8.GetBytes(match, 0, Math.Min(match.Length, 32), bytes, 0);
        return Vector256.Create(bytes);
    }
}
