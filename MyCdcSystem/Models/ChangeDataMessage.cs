using NpgsqlTypes;

namespace MyCdcSystem.Models
{
    public class ChangeDataMessage
    {
        public string Operation { get; set; } = default;
        public string Schema { get; set; } = default;
        public string Table { get; set; } = default;
        public Dictionary<string, object>? OldValues { get; set; }
        public Dictionary<string, object>? NewValues { get; set; }
        public DateTime Timestamp { get; set; }
        public uint? TransactionXid { get; set; }
        public NpgsqlLogSequenceNumber CommitLsn { get; set; }
        public DateTime TransactionCommitTimestamp { get; set; }

        public override string ToString()
        {
            var oldValuesString = OldValues != null
                ? string.Join(", ", OldValues.Select(kvp => $"{kvp.Key}: {kvp.Value}"))
                : "null";

            var newValuesString = NewValues != null
                ? string.Join(", ", NewValues.Select(kvp => $"{kvp.Key}: {kvp.Value}"))
                : "null";

            return $"Operation: {Operation}, Schema: {Schema}, Table: {Table}, " +
                   $"OldValues: {{ {oldValuesString} }}, NewValues: {{ {newValuesString} }}, " +
                   $"Timestamp: {Timestamp}, TransactionXid: {TransactionXid ?? 0}, " +
                   $"CommitLsn: {CommitLsn}, TransactionCommitTimestamp: {TransactionCommitTimestamp}";
        }
    }
}
