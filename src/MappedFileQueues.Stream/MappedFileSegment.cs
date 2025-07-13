using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;

namespace MappedFileQueues.Stream;

[DebuggerDisplay(
    "FileName = {FileName}, Capacity = {_viewStream.Capacity}, Position = {_viewStream.Position}, Remaining = {_viewStream.Capacity - _viewStream.Position}")]
internal class MappedFileSegment : IDisposable
{
    private readonly FileStream _fileStream;
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewStream _viewStream;

    private MappedFileSegment(
        string filePath,
        long fileSize,
        long viewStartOffset,
        bool readOnly)
    {
        if (fileSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fileSize), "File size must be greater than zero.");
        }

        _fileStream = new FileStream(
            filePath,
            readOnly ? FileMode.Open : FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.ReadWrite);

        _mmf = MemoryMappedFile.CreateFromFile(
            _fileStream,
            null,
            fileSize,
            MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None,
            true);

        _viewStream = _mmf.CreateViewStream(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        if (viewStartOffset > 0)
        {
            _viewStream.Seek(viewStartOffset, SeekOrigin.Begin);
        }
    }

    public string FileName => Path.GetFileName(_fileStream.Name);

    public bool HasEnoughSpace(int byteCount) => _viewStream.Position + byteCount <= _viewStream.Capacity;

    public void Write(ReadOnlySpan<byte> buffer)
    {
        if (!HasEnoughSpace(buffer.Length))
        {
            throw new InvalidOperationException(
                $"Not enough space in the mapped file segment {FileName}, required: {buffer.Length}, available: {_viewStream.Capacity - _viewStream.Position}.");
        }

        _viewStream.Write(buffer);
    }

    public void Write(byte byteValue)
    {
        if (!HasEnoughSpace(1))
        {
            throw new InvalidOperationException(
                $"Not enough space in the mapped file segment {FileName}, required: 1, available: {_viewStream.Capacity - _viewStream.Position}.");
        }

        _viewStream.WriteByte(byteValue);
    }

    /// <summary>
    /// Reads a specified number of bytes into the provided buffer.
    /// The position of the stream is advanced by the size of the buffer.
    /// </summary>
    /// <param name="buffer"> The buffer to read into.</param>
    /// <exception cref="InvalidOperationException">Thrown if there is not enough space in the mapped file segment to read the specified buffer.</exception>
    public void Read(Span<byte> buffer)
    {
        if (!HasEnoughSpace(buffer.Length))
        {
            throw new InvalidOperationException(
                $"Not enough space in the mapped file segment {FileName}, required: {buffer.Length}, available: {_viewStream.Capacity - _viewStream.Position}.");
        }

        _viewStream.ReadExactly(buffer);
    }

    /// <summary>
    /// Rewinds the stream by a specified number of bytes.
    /// </summary>
    /// <param name="step">The number of bytes to rewind.</param>
    /// <exception cref="InvalidOperationException">Thrown if the rewind operation would move the position before the start of the stream.</exception>
    public void Rewind(long step)
    {
        if (_viewStream.Position - step < 0)
        {
            throw new InvalidOperationException("Cannot rewind beyond the start of the stream.");
        }

        _viewStream.Seek(-step, SeekOrigin.Current);
    }

    public void Dispose()
    {
        _viewStream.Dispose();
        _mmf.Dispose();
        _fileStream.Dispose();
    }

    /// <summary>
    /// Creates a new <see cref="MappedFileSegment"/> instance based on the specified parameters.
    /// </summary>
    /// <param name="directory">The directory path where the files are stored.</param>
    /// <param name="fileSize">The size of the file, may be adjusted to fit the data type.</param>
    /// <param name="fileStartOffset">The offset of the first item stored in the file, which will determine the file name.</param>
    /// <returns>A new instance of <see cref="MappedFileSegment"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the file size is less than or equal to zero.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the file already exists.</exception>
    public static MappedFileSegment Create(
        string directory,
        long fileSize,
        long fileStartOffset)
    {
        if (fileSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fileSize), "File size must be greater than zero.");
        }

        var fileName = fileStartOffset.ToString("D20");

        var filePath = Path.Combine(directory, fileName);

        if (!Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        if (File.Exists(filePath))
        {
            throw new InvalidOperationException($"File '{filePath}' already exists. Cannot create a new segment.");
        }

        return new MappedFileSegment(
            filePath,
            fileSize,
            0,
            readOnly: false);
    }

    /// <summary>
    /// Tries to find a <see cref="MappedFileSegment"/> instance based on the specified parameters.
    /// </summary>
    /// <param name="directory">The directory path where the files are stored.</param>
    /// <param name="fileSize">The size of the file, may be adjusted to fit the data type.</param>
    /// <param name="offset">The offset of the item stored in the file.</param>
    /// <param name="segment">The found segment, or null if not found.</param>
    /// <returns>True if the segment was found; otherwise, false.</returns>
    public static bool TryFind(
        string directory,
        long fileSize,
        long offset,
        [MaybeNullWhen(false)] out MappedFileSegment segment)
    {
        if (!TryFindFile(directory, fileSize, offset, out var fileStartOffset))
        {
            segment = null;
            return false;
        }

        var fileName = fileStartOffset.ToString("D20");

        var filePath = Path.Combine(directory, fileName);

        segment = new MappedFileSegment(
            filePath,
            fileSize,
            offset - fileStartOffset,
            readOnly: true);
        return true;
    }

    private static bool TryFindFile(string directory, long fileSize, long offset, out long fileStartOffset)
    {
        fileStartOffset = 0;

        if (!Directory.Exists(directory))
        {
            // The directory may not be created yet
            return false;
        }

        var segmentFiles = Directory.GetFiles(directory);

        if (segmentFiles.Length == 0)
        {
            return false;
        }

        var startOffsets = segmentFiles
            .Select(file => long.Parse(Path.GetFileNameWithoutExtension(file)))
            .OrderDescending()
            .ToArray();

        foreach (var startOffset in startOffsets)
        {
            if (startOffset > offset)
            {
                continue;
            }

            if (offset >= startOffset + fileSize)
            {
                // The target segment file has not been created yet
                return false;
            }

            fileStartOffset = startOffset;
            return true;
        }

        return false;
    }
}
