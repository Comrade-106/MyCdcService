namespace MyCdcSystem.Models
{
    public class State
    {
        public uint? TransactionXid { get; set; } = default(uint?);
        public string Position { get; set; } = default;
        public DateTime LastProcessedTimestamp { get; set; } = DateTime.MinValue;
    }
}
