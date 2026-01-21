using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace IceForRocks;

public class BinaryStore<T>
    where T : unmanaged
{
    private readonly int _recordSize;

    public BinaryStore()
    {
        _recordSize = Marshal.SizeOf<T>();
    }

    // SAVING: Write a list of records to a binary file
    public void Save(string filePath, IEnumerable<T> records)
    {
        using var stream = new FileStream(
            filePath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None
        );
        using var writer = new BinaryWriter(stream);

        foreach (var record in records)
        {
            byte[] buffer = new byte[_recordSize];
            IntPtr ptr = Marshal.AllocHGlobal(_recordSize);
            try
            {
                Marshal.StructureToPtr(record, ptr, false);
                Marshal.Copy(ptr, buffer, 0, _recordSize);
                writer.Write(buffer);
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
        }
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
}
