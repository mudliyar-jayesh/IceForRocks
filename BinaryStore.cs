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
}
