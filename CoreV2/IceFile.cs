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
    private readonly int _headerSize = sizeof(long);
    
    public long Capacity { get; private set;}
    public string? FilePath { get; set; }

    
    public T* BasePointer => (T*)(_basePtr + _headerSize);

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
        
        var maxCap = Math.Max(defaultCapacity, _headerSize);
        if (_stream.Length < maxCap)
        {
            _stream.SetLength(maxCap);
        }
        Capacity = _stream.Length;

        MapInternal();
        
        _position = _headerSize + ((long)Count * _recordSize);
    }

    public int Count
    {
        get => (int)(*(long*)_basePtr);
        private set => *(long*)_basePtr = value;
    }
    
    public void Append(T item)
    {
        if (_position + _recordSize > Capacity)
        {
            Resize(Capacity * 2);
        }

        *(T*)(_basePtr + _position) = item;
        _position += _recordSize;
        Count++;
    }
    
    private void MapInternal()
    {
        _file = MemoryMappedFile.CreateFromFile(_stream!, null, Capacity, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false);
        _view = _file.CreateViewAccessor(0, Capacity);
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref _basePtr);
    }
    
    public void Commit()
    {
        _view?.Flush();
        _stream?.Flush();
    }

    private void Resize(long updatedCapcity)
    {
        if (_basePtr != null)
        {
            _view!.SafeMemoryMappedViewHandle.ReleasePointer();
        }

        _view?.Dispose();
        _file?.Dispose();
        _stream?.Dispose();

        _stream = new FileStream(FilePath!, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        _stream.SetLength(updatedCapcity);
        Capacity = updatedCapcity;

        MapInternal();
    }

    public void Dispose()
    {
        Commit();
        if (_basePtr != null)
        {
            _view!.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        _view?.Dispose();
        _file?.Dispose();
        _stream?.Dispose();
    }
}