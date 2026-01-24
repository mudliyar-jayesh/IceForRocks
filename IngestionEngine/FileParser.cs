using System.Threading.Channels;

namespace IceForRocks.Ingestion;

public class FileParser<T>
    where T : unmanaged
{
    private const int ChannelCapacity = 5000;

    public async Task ParseCSVFile(
        StreamReader streamReader,
        string dbPath,
        Func<string, T> parseMethod
    )
    {
        var channel = Channel.CreateBounded<T[]>(
            new BoundedChannelOptions(ChannelCapacity)
            {
                SingleWriter = true,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
            }
        );

        var writerTask = Task.Run(() => WriteToDisk(channel.Reader, dbPath));

        await ReadFromFile(channel.Writer, streamReader, parseMethod);
        await writerTask;
    }

    private async Task ReadFromFile(
        ChannelWriter<T[]> writer,
        StreamReader streamReader,
        Func<string, T> parseMethod
    )
    {
        var results = new List<T>(4096);

        string line;
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

    private async Task WriteToDisk(ChannelReader<T[]> reader, string dbPath)
    {
        // TODO: add masker later to methods for this line
        using var freezer = new IceFreezer<T>(dbPath, r => 0);

        await foreach (var batch in reader.ReadAllAsync())
        {
            foreach (var record in batch)
            {
                freezer.Append(record);
            }
        }

        freezer.Close();
    }
}
