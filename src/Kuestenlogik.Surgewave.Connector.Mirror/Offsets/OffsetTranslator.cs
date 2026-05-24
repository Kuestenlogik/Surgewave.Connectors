using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Offsets;

/// <summary>
/// In-memory offset translator with interpolation support.
/// Stores recent offset mappings for source-to-target offset translation.
/// </summary>
public sealed class OffsetTranslator : IOffsetTranslator
{
    private readonly ConcurrentDictionary<string, OffsetMappings> _mappings = new();
    private readonly int _maxMappingsPerPartition;

    public OffsetTranslator(int maxMappingsPerPartition = 10000)
    {
        _maxMappingsPerPartition = maxMappingsPerPartition;
    }

    public void StoreMapping(string sourceCluster, string topic, int partition, long sourceOffset, long targetOffset)
    {
        var key = MakeKey(sourceCluster, topic, partition);
        var mappings = _mappings.GetOrAdd(key, _ => new OffsetMappings(_maxMappingsPerPartition));
        mappings.Add(sourceOffset, targetOffset);
    }

    public long? Translate(string sourceCluster, string topic, int partition, long sourceOffset)
    {
        var key = MakeKey(sourceCluster, topic, partition);
        if (!_mappings.TryGetValue(key, out var mappings))
            return null;

        return mappings.Translate(sourceOffset);
    }

    public long? GetLatestTargetOffset(string sourceCluster, string topic, int partition)
    {
        var key = MakeKey(sourceCluster, topic, partition);
        if (!_mappings.TryGetValue(key, out var mappings))
            return null;

        return mappings.LatestTargetOffset;
    }

    public long? GetLatestSourceOffset(string sourceCluster, string topic, int partition)
    {
        var key = MakeKey(sourceCluster, topic, partition);
        if (!_mappings.TryGetValue(key, out var mappings))
            return null;

        return mappings.LatestSourceOffset;
    }

    private static string MakeKey(string sourceCluster, string topic, int partition)
        => $"{sourceCluster}:{topic}:{partition}";

    private sealed class OffsetMappings
    {
        private readonly SortedList<long, long> _sourceToTarget = new();
        private readonly object _lock = new();
        private readonly int _maxSize;

        public OffsetMappings(int maxSize)
        {
            _maxSize = maxSize;
        }

        public long? LatestSourceOffset
        {
            get
            {
                lock (_lock)
                {
                    return _sourceToTarget.Count > 0 ? _sourceToTarget.Keys[^1] : null;
                }
            }
        }

        public long? LatestTargetOffset
        {
            get
            {
                lock (_lock)
                {
                    return _sourceToTarget.Count > 0 ? _sourceToTarget.Values[^1] : null;
                }
            }
        }

        public void Add(long sourceOffset, long targetOffset)
        {
            lock (_lock)
            {
                _sourceToTarget[sourceOffset] = targetOffset;

                // Evict old mappings to bound memory
                while (_sourceToTarget.Count > _maxSize)
                {
                    _sourceToTarget.RemoveAt(0);
                }
            }
        }

        public long? Translate(long sourceOffset)
        {
            lock (_lock)
            {
                if (_sourceToTarget.Count == 0)
                    return null;

                // Exact match
                if (_sourceToTarget.TryGetValue(sourceOffset, out var exactMatch))
                    return exactMatch;

                // Binary search for interpolation
                var keys = _sourceToTarget.Keys;
                var values = _sourceToTarget.Values;

                var index = BinarySearch(keys, sourceOffset);
                if (index < 0)
                {
                    index = ~index;

                    // Before first mapping - use first target offset
                    if (index == 0)
                        return values[0];

                    // After last mapping - use last target offset + delta estimate
                    if (index >= _sourceToTarget.Count)
                    {
                        var lastSource = keys[^1];
                        var lastTarget = values[^1];
                        // Assume 1:1 mapping beyond known range
                        return lastTarget + (sourceOffset - lastSource);
                    }

                    // Interpolate between two nearest mappings
                    var lowerSource = keys[index - 1];
                    var upperSource = keys[index];
                    var lowerTarget = values[index - 1];
                    var upperTarget = values[index];

                    if (upperSource == lowerSource)
                        return lowerTarget;

                    var ratio = (double)(sourceOffset - lowerSource) / (upperSource - lowerSource);
                    return lowerTarget + (long)(ratio * (upperTarget - lowerTarget));
                }

                return values[index];
            }
        }

        private static int BinarySearch(IList<long> list, long value)
        {
            int lo = 0;
            int hi = list.Count - 1;

            while (lo <= hi)
            {
                int mid = lo + (hi - lo) / 2;
                var midVal = list[mid];

                if (midVal == value)
                    return mid;
                if (midVal < value)
                    lo = mid + 1;
                else
                    hi = mid - 1;
            }

            return ~lo;
        }
    }
}
