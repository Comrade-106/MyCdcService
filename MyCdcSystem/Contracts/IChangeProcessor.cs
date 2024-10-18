using MyCdcSystem.Models;

namespace MyCdcSystem.Contracts
{
    public interface IChangeProcessor
    {
        ChangeDataMessage? ProcessChanges(ChangeDataMessage message);
    }
}
