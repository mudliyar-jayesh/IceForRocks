using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace IceForRocks.Core.Storage;

public unsafe class IceSheet : IDisposable
{
    private readonly string _path;
    private FileStream _stream;
    private MemoryMappedFile _file;
    private MemoryMappedViewAccessor _view;
    private byte* _basePtr;
    private readonly long _capacity;

    private const long _alignment = 64 * 1024;
    
    public byte* BasePtr => _basePtr;
    public long Capacity => _capacity;

    public IceSheet(string path, long requestedCapacity)
    {
        _path = path;
        _capacity = (requestedCapacity + _alignment - 1) & ~(_alignment - 1) ;

        _stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        if (_stream.Length < _capacity)
        {
            _stream.SetLength(_capacity);
        }

        _file = MemoryMappedFile.CreateFromFile(_stream, null, _capacity, MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, false);
        _view = _file.CreateViewAccessor(0, _capacity, MemoryMappedFileAccess.ReadWrite);

        byte* ptr = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        _basePtr = ptr;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* GetHandle(long offset)
    {
        return _basePtr + offset;
    }

    public void ColdFlush()
    {
        _view.Flush();
    }
    
    public void Dispose()
    {
        if (_basePtr != null)
        {
            _view?.SafeMemoryMappedViewHandle.ReleasePointer();
        }
        _view?.Dispose();
        _file?.Dispose();
        _stream?.Dispose();
    }
}