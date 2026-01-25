public unsafe class IceQuery<T>
{
    private readonly byte* _basePtr;
    private readonly int _recordSize;
    public List<int> Indices { get; private set; }

    public ulong SearchMask { get; set; }
    public Func<T, bool>? Predicate { get; set; }
    public Func<T, ulong>? BitmaskGenerator { get; set; }

    public IceQuery(byte* basePtr, int recordSize, int totalRecords)
    {
        _basePtr = basePtr;
        _recordSize = recordSize;
        Indices = Enumerable.Range(0, totalRecords).ToList();
    }

    public unsafe IceQuery<T> Filter(int offset, string value)
    {
        byte[] targetBytes = System.Text.Encoding.UTF8.GetBytes(value);
        ReadOnlySpan<byte> targetSpan = targetBytes.AsSpan();

        var visited = new List<int>(Indices.Count);

        foreach (var index in Indices)
        {
            byte* ptr = _basePtr + (index * _recordSize) + offset;
            if (BinaryComparisonHelper.SpanEquals(ptr, targetSpan))
            {
                visited.Add(index);
            }
        }
        Indices = visited;
        return this;
    }

    public unsafe IceQuery<T> SortByString(int offset, int fieldSize = 64)
    {
        Indices.Sort(
            (a, b) =>
            {
                byte* ptrA = _basePtr + (a * _recordSize) + offset;
                byte* ptrB = _basePtr + (b * _recordSize) + offset;
                return BinaryComparisonHelper.CompareBytes(ptrA, ptrB, fieldSize);
            }
        );
        return this;
    }

    public List<int> TakePage(int page, int pageSize)
    {
        int skip = (page - 1) * pageSize;
        return Indices.Skip(skip).Take(pageSize).ToList();
    }
}
