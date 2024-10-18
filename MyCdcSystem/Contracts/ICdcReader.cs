using MyCdcSystem.Models;

namespace MyCdcSystem.Contracts
{
    public interface ICdcReader
    {
        IAsyncEnumerable<ChangeDataMessage> ReadChangesAsync(CancellationToken token);
    }
}
