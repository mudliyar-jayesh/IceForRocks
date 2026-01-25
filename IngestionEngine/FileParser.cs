using System.Threading.Channels;
using IceForRocks.Core;

namespace IceForRocks.Ingestion;

public interface IFileParser<T>
{
    public void Setup(string dbath);
    public Task ParseCSVFile(
        StreamReader streamReader,
        Func<string, T> parseMethod,
        Func<T, ulong> bitMaskGenerator
    );
}

public class FileParser<T> : IFileParser<T>
    where T : unmanaged
{
    private const int ChannelCapacity = 5000;

    private string _dbPath = string.Empty;
    private Channel<T[]>? _batchChannel;

    public void Setup(string dbPath)
    {
        _dbPath = dbPath;
        _batchChannel = Channel.CreateBounded<T[]>(
            new BoundedChannelOptions(ChannelCapacity)
            {
                SingleWriter = true,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
            }
        );
    }

    public async Task ParseCSVFile(
        StreamReader streamReader,
        Func<string, T> parseMethod,
        Func<T, ulong> bitMaskGenerator
    )
    {
        if (_batchChannel is null)
        {
            throw new Exception("FileParser must be setup before use.");
        }
        var writerTask = Task.Run(() =>
            WriteToDisk(_batchChannel.Reader, _dbPath, bitMaskGenerator)
        );

        await ReadFromFile(_batchChannel!.Writer, streamReader, parseMethod);
        await writerTask;
    }

    private async Task ReadFromFile(
        ChannelWriter<T[]> writer,
        StreamReader streamReader,
        Func<string, T> parseMethod
    )
    {
        var results = new List<T>(4096);

        string? line;
        while ((line = await streamReader.ReadLineAsync()) != null)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            T record = parseMethod.Invoke(line);
            results.Add(record);

            if (results.Count >= 4096)
            {
                await writer.WriteAsync(results.ToArray());
                results.Clear();
            }
        }

        if (results.Count > 0)
        {
            await writer.WriteAsync(results.ToArray());
        }
        writer.Complete();
    }

    private async Task WriteToDisk(
        ChannelReader<T[]> reader,
        string dbPath,
        Func<T, ulong> bitmaskGenerator
    )
    {
        // TODO: add masker later to methods for this line
        using var freezer = new IceFreezer<T>(dbPath, bitmaskGenerator);

        await foreach (var batch in reader.ReadAllAsync())
        {
            foreach (var record in batch)
            {
                freezer.Append(record);
            }
        }

        freezer.Dispose();
    }
}
