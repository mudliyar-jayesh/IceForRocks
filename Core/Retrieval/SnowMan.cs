using IceForRocks.Core.Ingestion;
using IceForRocks.Core.Storage;
using IceForRocks.Core.Strings;

namespace IceForRocks.Core.Retrieval;

public class SnowMan : IDisposable
{
    private readonly string _basePath;
    private readonly List<IceSheet> _sheets = new();
    private readonly IceBreaker _breaker = new();

    public Dictionary<string, object> Columns { get; } = new();

    public SnowMan(string basePath)
    {
        _basePath = basePath;
        if (!Directory.Exists(_basePath))
        {
            Directory.CreateDirectory(_basePath);
        }
    }

    public void RegisterNumericColumn<T>(string name, int csvOffset) where T : unmanaged
    {
        var sheet = new IceSheet(Path.Combine(_basePath, $"{name}.icicle"), 1024 * 1024);
        var column = new Icicle<T>(sheet);
        
        _sheets.Add(sheet);
        Columns.Add(name, column);
        _breaker.AddIcicle(csvOffset, column);
    }

    public void RegisterSnowball(string name, int csvOffset)
    {
        var sheet = new IceSheet(Path.Combine(_basePath, $"{name}.snowball"), 1024 * 1024);
        var snowball = new Snowball(sheet);
        
        _sheets.Add(sheet);
        Columns.Add(name, snowball);
        _breaker.AddSnowball(csvOffset, snowball);
    }

    public void RegisterIceTray(string name, int csvOffset)
    {
        var heapSheet = new IceSheet(Path.Combine(_basePath, $"{name}.heap"),  4 * 1024 * 1024);
        var offsetSheet  = new IceSheet(Path.Combine(_basePath, $"{name}.offset"),  1024 * 1024);
        var offsets = new Icicle<long>(offsetSheet);
        
        var bloomSheet = new IceSheet(Path.Combine(_basePath, $"{name}.bloom"),  512 * 1024);
        var bloomFilter = new Snowball(bloomSheet);
        
        var tray = new IceTray(heapSheet, offsets,  bloomFilter);
        
        var idSheet = new IceSheet(Path.Combine(_basePath, $"{name}.ids"),  1024 * 1024);
        var idColumn = new Icicle<int>(idSheet);
        
        _sheets.Add(heapSheet);
        _sheets.Add(offsetSheet);
        _sheets.Add(bloomSheet);
        _sheets.Add(idSheet);
        
        Columns.Add(name, tray);
        Columns.Add($"{name}_IDs", idColumn);
        
        _breaker.AddIceTray(csvOffset, tray, idColumn);
    }

    public void Ingest(long rowIndex, ReadOnlySpan<byte> row)
    {
        _breaker.Break(rowIndex, row);
    }



    public void Dispose()
    {
        foreach (var sheet in _sheets)
        {
            sheet.Dispose();
        }
    }
}