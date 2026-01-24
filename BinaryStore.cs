using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace IceForRocks;

public class BinaryStore<T>
    where T : unmanaged
{
    private readonly int _recordSize;
    private const int RecordsPerSegment = 1024;

    public BinaryStore()
    {
        _recordSize = Marshal.SizeOf<T>();

        // Printing warnings (for dev)
        if (64 % _recordSize != 0 && _recordSize % 64 != 0)
        {
            Console.WriteLine(
                $"[Peformance WARNING]: Record size {_recordSize} is not aligned with 64-byte cache lines."
            );
        }
        if (_recordSize % 8 != 0)
        {
            Console.WriteLine(
                $"[Peformance WARNING]: Record size {_recordSize} is not 8-byte aligned. CPU performance will degrade"
            );
        }
    }

    /*
    DO NOT USE THIS.
    This is still experimental. A lot of testing is pending on this one, especially.

    */
    public List<T> SearchWithSIMD(string filePath, ulong searchMask, string targetName)
    {
        var results = new ConcurrentBag<T>();
        var fileInfo = new FileInfo(filePath);
        var targetVector = SIMDSearch.CreateTargetVector(targetName);

        using var mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
        int headerSize = Marshal.SizeOf<SegmentHeader>();
        long fileSize = fileInfo.Length;

        using (var accessor = mmf.CreateViewAccessor())
        {
            accessor.Read(fileSize - 4, out int segmentCount);
            long headersOffset = fileSize - 4 - (segmentCount * headerSize);

            Parallel.ForEach(
                Partitioner.Create(0, segmentCount),
                range =>
                {
                    using var va = mmf.CreateViewAccessor();
                    unsafe
                    {
                        try
                        {
                            byte* basePtr = null;
                            va.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
                            for (int s = range.Item1; s < range.Item2; s++)
                            {
                                // 1. Bitmask Skip (The Gatekeeper)
                                SegmentHeader header;
                                accessor.Read(headersOffset + (s * headerSize), out header);
                                if ((header.Bitmask & searchMask) == 0)
                                    continue;

                                // 2. SIMD Scan (The Hot Path)
                                long segStart = (long)s * RecordsPerSegment * _recordSize;
                                for (int i = 0; i < RecordsPerSegment; i++)
                                {
                                    byte* currentRecordPtr = basePtr + segStart + (i * _recordSize);

                                    // We assume 'Name' is the first field at Offset 0
                                    if (SIMDSearch.Equals32(currentRecordPtr, targetVector))
                                    {
                                        results.Add(*(T*)currentRecordPtr);
                                    }
                                }
                            }
                        }
                        finally
                        {
                            va.SafeMemoryMappedViewHandle.ReleasePointer();
                        }
                    }
                }
            );
        }
        return results.ToList();
    }

    /*
    [basicallly, fast save . bit masking]
    here is an example on how to create a bit mask ,
    public static class UserFilters
    {
        public const ulong IsActive = 1UL << 0;       // Bit 0
        public const ulong IsAdmin = 1UL << 1;        // Bit 1
        public const ulong HasParent = 1UL << 2;      // Bit 2
        public const ulong InternalGroup = 1UL << 3;  // Bit 3
    }

    // bit mask generator
    ulong MyBitmaskGenerator(UserGroupRecord record)
    {
        ulong mask = 0;

        // Check if Active
        if (record.IsActive == 1)
            mask |= UserFilters.IsActive;

        // Custom logic: check if the Name starts with "ADM" (Admin)
        // Using a helper to read the fixed byte array
        if (BinaryHelper.ReadString(record.Name, 64).StartsWith("ADM"))
            mask |= UserFilters.IsAdmin;

        // Check if Parent is not empty
        if (record.Parent[0] != 0)
            mask |= UserFilters.HasParent;

        return mask;
    }

    // now the search
    var store = new BinaryStore<UserGroupRecord>();

    // We want records where BOTH Active (Bit 0) and Admin (Bit 1) COULD exist.
    ulong searchCriteria = UserFilters.IsActive | UserFilters.IsAdmin;

    var results = store.Search(
        "users.bin",
        searchCriteria,
        record => record.IsActive == 1 && BinaryHelper.ReadString(record.Name, 64).StartsWith("ADM")
    );

    */
    public void Save(string filePath, List<T> records, Func<T, ulong> bitmaskGenerator)
    {
        int count = records.Count;
        int segmentCount = (int)Math.Ceiling((double)count / RecordsPerSegment);

        // bitmasks for each segment
        var segmentHeaders = new SegmentHeader[segmentCount];
        var span = CollectionsMarshal.AsSpan(records);

        for (int i = 0; i < segmentCount; i++)
        {
            ulong mask = 0;
            int start = i * RecordsPerSegment;
            int end = Math.Min(start + RecordsPerSegment, count);

            for (int j = start; j < end; j++)
            {
                mask |= bitmaskGenerator(span[j]);
            }
            segmentHeaders[i] = new SegmentHeader { Bitmask = mask };
        }

        // write data and headers  (bitmask)
        using var stream = new FileStream(filePath, FileMode.Create);
        using var writer = new BinaryWriter(stream);

        // first write the data
        byte[] data = new byte[count * _recordSize];
        MemoryMarshal.AsBytes(span).CopyTo(data);
        writer.Write(data);

        // then write the headers
        byte[] headerData = new byte[segmentHeaders.Length * Marshal.SizeOf<SegmentHeader>()];
        MemoryMarshal.AsBytes(segmentHeaders.AsSpan()).CopyTo(headerData);
        writer.Write(headerData);

        // add segment count to end, as meta data

        writer.Write(segmentCount);
    }

    public List<T> Search(string filePath, ulong searchMask, Func<T, bool> predicate)
    {
        var results = new ConcurrentBag<T>();
        var fileInfo = new FileInfo(filePath);

        using var memMap = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);

        // read meta data
        using (var accessor = memMap.CreateViewAccessor())
        {
            int headerSize = Marshal.SizeOf<SegmentHeader>();
            long fileSize = fileInfo.Length;

            // read segment count (last 4 bytes)
            int segmentCount;
            accessor.Read(fileSize - 4, out segmentCount);

            long headerOffset = fileSize - 4 - (segmentCount * headerSize);

            // use bitmask filter to search for the record
            Parallel.ForEach(
                Partitioner.Create(0, segmentCount),
                range =>
                {
                    using var segAccessor = memMap.CreateViewAccessor();

                    for (int s = range.Item1; s < range.Item2; s++)
                    {
                        SegmentHeader header;
                        segAccessor.Read(headerOffset + (s * headerSize), out header);

                        // skip if not matching our search
                        if ((header.Bitmask & searchMask) == 0)
                        {
                            continue;
                        }

                        // found segment to scan
                        int start = s * RecordsPerSegment;
                        for (int i = 0; i < RecordsPerSegment; i++)
                        {
                            T record;
                            segAccessor.Read((start + i) * _recordSize, out record);
                            if (predicate(record))
                            {
                                results.Add(record);
                            }
                        }
                    }
                }
            );
        }
        return results.ToList();
    }

    // SAVING: Write a list of records to a binary file
    public void Save(string filePath, List<T> records)
    {
        int count = records.Count;
        byte[] buffer = new byte[count * _recordSize];

        MemoryMarshal.AsBytes(CollectionsMarshal.AsSpan(records)).CopyTo(buffer);

        string tempFile = $"{filePath}.tmp";
        File.WriteAllBytes(tempFile, buffer);
        File.Move(tempFile, filePath, overwrite: true); // avoids file corruption chances
    }

    // SEARCHING: Scan the file in parallel using a generic predicate
    public List<T> Search(string filePath, Func<T, bool> predicate)
    {
        var results = new ConcurrentBag<T>();
        var fileInfo = new FileInfo(filePath);
        if (!fileInfo.Exists)
            return new List<T>();

        long totalRecords = fileInfo.Length / _recordSize;

        using var mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);

        Parallel.ForEach(
            Partitioner.Create(0, totalRecords),
            range =>
            {
                using var accessor = mmf.CreateViewAccessor(
                    range.Item1 * _recordSize,
                    (range.Item2 - range.Item1) * _recordSize,
                    MemoryMappedFileAccess.Read
                );

                for (long i = 0; i < (range.Item2 - range.Item1); i++)
                {
                    accessor.Read(i * _recordSize, out T record);
                    if (predicate(record))
                    {
                        results.Add(record);
                    }
                }
            }
        );

        return results.ToList();
    }

    /*
    Updated Batched Search for better and faster reads.
    */
    public List<T> Read(
        string filePath,
        Func<T, bool> predicate,
        Func<T, object> orderBy = null,
        bool ascending = true,
        int page = 1,
        int pageSize = 50
    )
    {
        var filteredMatches = new List<T>();

        foreach (var batch in BatchStream(filePath, 20_000))
        {
            foreach (var record in batch)
            {
                if (predicate(record))
                {
                    filteredMatches.Add(record);
                }
            }
        }

        IEnumerable<T> query = filteredMatches;
        if (orderBy != null)
        {
            query = ascending ? query.OrderBy(orderBy) : query.OrderByDescending(orderBy);
        }

        return query.Skip((page - 1) * pageSize).Take(pageSize).ToList();
    }

    public IEnumerable<T[]> BatchStream(string filePath, int batchSize = 10000)
    {
        var fileInfo = new FileInfo(filePath);
        if (!fileInfo.Exists)
        {
            yield break;
        }

        long totalRecords = fileInfo.Length / _recordSize;
        using var memMap = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);

        using var accessor = memMap.CreateViewAccessor(
            0,
            fileInfo.Length,
            MemoryMappedFileAccess.Read
        );

        for (long i = 0; i < totalRecords; i += batchSize)
        {
            int currentBatchSize = (int)Math.Min(batchSize, totalRecords - i);
            T[] batch = new T[currentBatchSize];

            CopyBatchUnsafe(accessor, i, batch);

            yield return batch;
        }
    }

    private unsafe void CopyBatchUnsafe(
        MemoryMappedViewAccessor accessor,
        long startRecord,
        T[] destination
    )
    {
        byte* basePtr = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);

        try
        {
            byte* startPtr = basePtr + (startRecord * _recordSize);

            fixed (T* destPtr = destination)
            {
                var copySize = destination.Length * _recordSize;
                Buffer.MemoryCopy(startPtr, destPtr, copySize, copySize);
            }
        }
        finally
        {
            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    public unsafe List<int> FilterByField<TValue>(
        byte* basePtr,
        int totalRecords,
        int recordSize,
        int fieldOffset,
        TValue targetValue
    )
        where TValue : unmanaged
    {
        var matchingIndices = new List<int>();
        for (int i = 0; i < totalRecords; i++)
        {
            void* fieldPtr = (byte*)basePtr + (i * recordSize) + fieldOffset;

            if (EqualityComparer<TValue>.Default.Equals(*(TValue*)fieldPtr, targetValue))
            {
                matchingIndices.Add(i);
            }
        }
        return matchingIndices;
    }
}
