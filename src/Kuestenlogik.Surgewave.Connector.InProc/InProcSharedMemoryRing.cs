using System.IO.MemoryMappedFiles;

namespace Kuestenlogik.Surgewave.Connector.InProc;

/// <summary>
/// Layout of the single-producer/single-consumer ring buffer shared by the InProc sink
/// and source. The first <see cref="HeaderSize"/> bytes hold the write and read cursors
/// (absolute byte counts that never wrap); the rest of the mapping holds length-prefixed
/// messages. The writer may only reuse space the reader has committed past, so a full
/// buffer blocks the writer instead of silently overwriting unread messages.
/// </summary>
internal static class InProcSharedMemoryRing
{
    /// <summary>Byte offset of the writer cursor within the mapping.</summary>
    public const int WritePositionOffset = 0;

    /// <summary>Byte offset of the reader cursor within the mapping.</summary>
    public const int ReadPositionOffset = 8;

    /// <summary>Bytes reserved for the cursors in front of the data region.</summary>
    public const int HeaderSize = 16;

    /// <summary>Bytes of length prefix in front of every message.</summary>
    public const int LengthPrefixSize = 4;

    /// <summary>Usable data bytes of a mapping, i.e. everything behind the header.</summary>
    public static int CapacityOf(MemoryMappedViewAccessor accessor)
    {
        var total = accessor.Capacity;
        return (int)Math.Min(total, int.MaxValue) - HeaderSize;
    }

    /// <summary>Writes into the data region at an absolute position, wrapping at the end.</summary>
    public static void Write(MemoryMappedViewAccessor accessor, int capacity, long position,
        byte[] data, int offset, int count)
    {
        var start = (int)(position % capacity);
        var first = Math.Min(count, capacity - start);

        accessor.WriteArray(HeaderSize + start, data, offset, first);

        if (first < count)
            accessor.WriteArray(HeaderSize, data, offset + first, count - first);
    }

    /// <summary>Reads from the data region at an absolute position, wrapping at the end.</summary>
    public static void Read(MemoryMappedViewAccessor accessor, int capacity, long position,
        byte[] data, int offset, int count)
    {
        var start = (int)(position % capacity);
        var first = Math.Min(count, capacity - start);

        accessor.ReadArray(HeaderSize + start, data, offset, first);

        if (first < count)
            accessor.ReadArray(HeaderSize, data, offset + first, count - first);
    }
}
