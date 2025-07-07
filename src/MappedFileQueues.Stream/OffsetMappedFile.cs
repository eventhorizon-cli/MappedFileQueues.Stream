using System.IO.MemoryMappedFiles;

namespace MappedFileQueues.Stream;

internal class OffsetMappedFile : IDisposable
{
    private readonly FileStream _fileStream;
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _vierAccessor;

    private long _offset;

    public OffsetMappedFile(string filePath)
    {
        _fileStream = new FileStream(
            filePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.ReadWrite);

        _mmf = MemoryMappedFile.CreateFromFile(
            _fileStream,
            null,
            sizeof(long),
            MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None,
            true);

        _vierAccessor = _mmf.CreateViewAccessor(0, sizeof(long), MemoryMappedFileAccess.ReadWrite);
        _vierAccessor.Read(0, out _offset);
    }

    public long Offset => _offset;

    public void Advance(long step)
    {
        _offset += step;
        _vierAccessor.Write(0, _offset);
    }

    public void AdvanceTo(long offset)
    {
        _offset = offset;
        _vierAccessor.Write(0, offset);
    }

    public void Dispose()
    {
        _fileStream.Dispose();
        _mmf.Dispose();
        _vierAccessor.Dispose();
    }
}
