
namespace MyCdcSystem.Models.Configurations
{
    public class PostgresConfiguration
    {
        public string ConnectionString { get; set; }
        public string SlotName { get; set; }
        public string PublicationName { get; set; }
        public string TableName { get; set; }
    }
}
