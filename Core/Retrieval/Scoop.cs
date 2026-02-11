using System.Runtime.Intrinsics;
using IceForRocks.Core.Storage;

namespace IceForRocks.Core.Retrieval;

public unsafe class Scoop
{

    public List<long> Filter<T>(Icicle<T> column, T target, ScanOp op, long rowCount) where T : unmanaged, IComparable<T>
    {
        var results = new  List<long>();
        for (long i = 0; i < rowCount; i++)
        {
            T value = column.Peek(i);
            bool match = op switch
            {
                ScanOp.Equals => value.CompareTo(target) == 0,
                ScanOp.GreaterThan => value.CompareTo(target) > 0,
                ScanOp.LessThan => value.CompareTo(target) < 0,
                _ => false
            };

            if (match)
            {
                results.Add(i);
            }
        }

        return results;
    }

    public double Sum(Icicle<double> column, List<long> selection)
    {
        double total = 0;
        foreach (var index in selection)
        {
            total +=  column.Peek(index);
        }

        return total;
    }

    public List<long> SearchString(Icicle<int> idColumn, int symbolId, long rowCount)
    {
        var results = new  List<long>();
        int vectorSize = Vector128<int>.Count;
        var targetVec = Vector128.Create(symbolId);
        
        long i = 0;
        for (; i <= rowCount - vectorSize; i += vectorSize)
        {
            var dataVec = Vector128.Load((int*)idColumn.Peek(i * sizeof(int)));
            if (Vector128.EqualsAny(dataVec, targetVec))
            {
                for (int j = 0; j < vectorSize; j++)
                {
                    if (idColumn.Peek(i + j) == symbolId)
                    {
                        results.Add(i + j);
                    }
                }
            }
        }
        return results;
    }
}

public enum ScanOp { Equals, GreaterThan, LessThan }