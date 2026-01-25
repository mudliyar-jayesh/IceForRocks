using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;

namespace IceForRocks.Core;

public delegate void RefAction<T>(ref T item);

public class IceBreaker<T> : IDisposable
    where T : unmanaged
{
    private readonly MemoryMappedFile _dataMemMap;
    private readonly MemoryMappedFile _idxMemMap;

    private readonly MemoryMappedViewAccessor _dataAccessor;
    private readonly MemoryMappedViewAccessor _idxAccessor;

    private unsafe byte* _dataBasePtr = null;
    private unsafe byte* _idxBasePtr = null;

    private readonly int _dataSize;
    private readonly int _idxSize;
    private readonly int _segmentCount;

    private readonly long _dataFileSize;
    private readonly long _idxFileSize;

    public unsafe IceBreaker(string filePath)
    {
        _dataSize = Marshal.SizeOf<T>();
        _idxSize = Marshal.SizeOf<SegmentHeader>();

        var dataDirectory = Path.GetDirectoryName(filePath)!;
        if (!Directory.Exists(dataDirectory))
        {
            Directory.CreateDirectory(dataDirectory);
        }

        string idxFilePath = Path.Combine(
            dataDirectory,
            $"{Path.GetFileNameWithoutExtension(filePath)}_idx.bin"
        );

        var idxFileInfo = new FileInfo(idxFilePath);
        _idxFileSize = idxFileInfo.Length;
        var dataFileInfo = new FileInfo(filePath);
        _dataFileSize = dataFileInfo.Length;

        _idxMemMap = MemoryMappedFile.CreateFromFile(
            idxFilePath,
            FileMode.Open,
            null,
            0,
            MemoryMappedFileAccess.ReadWrite
        );
        _dataMemMap = MemoryMappedFile.CreateFromFile(
            filePath,
            FileMode.Open,
            null,
            0,
            MemoryMappedFileAccess.ReadWrite
        );

        _dataAccessor = _dataMemMap.CreateViewAccessor(
            0,
            _dataFileSize,
            MemoryMappedFileAccess.ReadWrite
        );
        _idxAccessor = _idxMemMap.CreateViewAccessor(
            0,
            _idxFileSize,
            MemoryMappedFileAccess.ReadWrite
        );

        _dataAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _dataBasePtr);
        _idxAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _idxBasePtr);

        _segmentCount = (int)_idxFileSize / _idxSize;
    }

    public unsafe void ApplyUpdate(IceQuery<T> query, RefAction<T> updateAction)
    {
        for (int idx = 0; idx < _segmentCount; idx++)
        {
            SegmentHeader* headerPtr = (SegmentHeader*)(_idxBasePtr + (idx * _idxSize));

            if (query.SearchMask != 0 && (headerPtr->Bitmask & query.SearchMask) == 0)
            {
                continue;
            }

            long dataStartLocation = (long)idx * _dataSize;
            T* recordPtr = (T*)(_dataBasePtr + dataStartLocation);

            if (query.Predicate != null && query.Predicate(*recordPtr))
            {
                updateAction(ref *recordPtr);
                ulong newMask = 0;
                newMask |= query.BitmaskGenerator(*recordPtr);
                headerPtr->Bitmask = newMask;
            }
        }
    }

    public unsafe List<T> Search(IceQuery<T> query)
    {
        var results = new List<T>();
        for (int idx = 0; idx < _segmentCount; idx++)
        {
            SegmentHeader* headerPtr = (SegmentHeader*)(_idxBasePtr + (idx * _idxSize));

            if (query.SearchMask != 0 && (headerPtr->Bitmask & query.SearchMask) == 0)
            {
                continue;
            }

            long dataStartLocation = (long)idx * _dataSize;
            T* recordPtr = (T*)(_dataBasePtr + dataStartLocation);

            if (query.Predicate != null && query.Predicate(*recordPtr))
            {
                results.Add(*recordPtr);
            }
        }
        return results;
    }

    public unsafe void Dispose()
    {
        if (_dataBasePtr != null)
        {
            _dataAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
            _dataBasePtr = null;
        }
        if (_idxBasePtr != null)
        {
            _idxAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
            _idxBasePtr = null;
        }

        _dataAccessor.Dispose();
        _dataMemMap.Dispose();

        _idxAccessor.Dispose();
        _idxMemMap.Dispose();
    }
}
