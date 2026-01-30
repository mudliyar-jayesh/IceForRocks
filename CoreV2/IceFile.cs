using System.IO.MemoryMappedFiles;

namespace IceForRocks.CoreV2;

public unsafe class IceFile<T> : IDisposable where T : unmanaged
{
    private MemoryMappedFile? _file;
    private MemoryMappedViewAccessor? _view;
    private FileStream? _stream;

    private byte* _basePtr;
    private long _position;
    private readonly int _recordSize;

    public long Capacity { get; private set;}
    public string? FilePath { get; set; }

    public IceFile(string path, long defaultCapacity = 1024 * 1024 * 10)
    {
        FilePath = path; 
        _recordSize = sizeof(T);

        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        if (_stream.Length < defaultCapacity)
        {
            _stream.SetLength(defaultCapacity);
        }
        Capacity = _stream.Length;

        MapInternal();
        
        _position = 0;
    }
    
    private void MapInternal()
    {
        _file = MemoryMappedFile.CreateFromFile(_stream!, null, Capacity, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false);
        _view = _file.CreateViewAccessor(0, Capacity);
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref _basePtr);
    }

    public void Append(T item)
    {
        if (_position + _recordSize > Capacity)
        {
            Resize(Capacity * 2); // basically extend like a dynammic list
        }

        *(T*)(_basePtr + _position) = item;
        _position += _recordSize;
    }

    public void Commit()
    {
        _stream?.Flush();
    }

    private void Resize(long updatedCapcity)
    {
        if (_basePtr != null)
        {
            _view!.SafeMemoryMappedViewHandle.ReleasePointer();
            _basePtr = null;
        }

        _view?.Dispose();
        _file?.Dispose();
        _stream?.Dispose();

        _stream = new FileStream(FilePath!, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        _stream.SetLength(updatedCapcity);
        Capacity = updatedCapcity;

        MapInternal();
    }


    public T* BasePointer => (T*)_basePtr;
    public int Count => (int) ((_position + 1) * _recordSize/ _recordSize);

    public void SetPosition(long count)
    {
        long requiredBytes = count * _recordSize;
        if (requiredBytes > Capacity)
        {
            Resize(requiredBytes + 1024 * 1024);
        }
        _position = requiredBytes;
    }

    public void Dispose()
    {
        if (_basePtr != null)
        {
            _view!.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        _view?.Dispose();
        _file?.Dispose();
        _stream?.Dispose();
    }

    public ReadOnlySpan<byte> GetDump()
    {
        return new ReadOnlySpan<byte>(_basePtr, Count * _recordSize);
    }
}