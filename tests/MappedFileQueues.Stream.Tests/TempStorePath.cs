namespace MappedFileQueues.Tests;

public class TempStorePath : IDisposable
{
    private TempStorePath(string path)
    {
        Path = path;
    }

    public string Path { get; }

    public void Dispose()
    {
        if (!Directory.Exists(Path))
        {
            return;
        }

        try
        {
            Directory.Delete(Path, true);
        }
        catch (IOException)
        {
            // Ignore IO exceptions during cleanup
        }
    }

    public static TempStorePath Create()
    {
        var path = "temp_" + Guid.NewGuid().ToString("N");
        return new TempStorePath(path);
    }
}
