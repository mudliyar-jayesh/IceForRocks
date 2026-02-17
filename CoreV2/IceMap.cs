using System.Text;

namespace IceForRocks.CoreV2;

public class IceMap: IDisposable
{
    /*
    okay, so forward and reverse are dumb names, but 
    that what i am going with. its 4:18 AM, deal with it.
    */
    private Dictionary<string, int> _forward = new ();
    private List<string> _reverse = new ();

    private readonly FileStream _stream;
    private readonly BinaryWriter _writer;
    private readonly object _lock = new();
    
    public List<string> GetValues() => _reverse;

    public IceMap(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        _stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        _writer = new BinaryWriter(_stream, Encoding.UTF8, leaveOpen: true);

        LoadExisting(path);

    }

    public int Get(string value)
    {
        if (string.IsNullOrEmpty(value) || string.IsNullOrWhiteSpace(value)  ||
            !_forward.ContainsKey(value))
        {
            return -1;
        }
        return _forward[value];
    }

    public int Add(string value)
    {
        if (string.IsNullOrEmpty(value) || string.IsNullOrWhiteSpace(value))
        {
            return -1;
        }

        if (_forward.ContainsKey(value))
        {
            return _forward[value];
        }
        
        lock (_lock)
        {
            int newId = _reverse.Count;
            _reverse.Add(value);
            _forward[value] = newId;

            _writer.Write(value);
            _writer.Flush();
            return newId;
        }
    }

    public string GetValue(int id)
    {
        if (id < 0 || id > _reverse.Count)
        {
            return string.Empty;
        }
        return _reverse[id];
    }

    private void LoadExisting(string path)
    {
        if (_stream.Length == 0)
        {
            return;
        }
        _stream.Position = 0;
        using var reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);
        while (_stream.Position < _stream.Length)
        {
            try
            {
                string value = reader.ReadString();

                 int id = _reverse.Count;
                _reverse.Add(value);
                _forward[value] = id;
            } 
            catch (EndOfStreamException) {break;}
        }
        _stream.Seek(0, SeekOrigin.End);
    }

    public void Dispose()
    {
        _writer?.Dispose();
        _stream?.Dispose();
    }
}