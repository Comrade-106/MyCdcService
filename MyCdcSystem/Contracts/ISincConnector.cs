using MyCdcSystem.Models;

namespace MyCdcSystem.Contracts
{
    public interface ISincConnector
    {
        public Task SendChangesAsync(ChangeDataMessage message);
    }
}
