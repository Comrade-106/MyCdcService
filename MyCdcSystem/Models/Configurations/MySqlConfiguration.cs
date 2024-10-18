
namespace MyCdcSystem.Models.Configurations
{
    public class MySqlConfiguration
    {
        public string ConnectionString { get; set; }
        public string BinlogFileName { get; set; }
        public string BinlogPosition { get; set; }

        public long GetBinlogPosition()
        {
            return long.TryParse(BinlogPosition, out var result) ? result : 0;
        }
    }
}
