namespace MyCdcSystem.Models.Configurations
{
    public class SimpleChangeProcessorConfiguration
    {
        public HashSet<string>? AllowedOperations { get; set; }
        public HashSet<string>? AllowedTables { get; set; }
        public Dictionary<string, HashSet<string>>? AllowedColumns { get; set; }
    }
}