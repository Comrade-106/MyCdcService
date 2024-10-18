using MyCdcSystem.Contracts;

namespace MyCdcSystem.Core.Pipeline
{
    public class DataPipeline
    {
        private readonly ICdcReader _reader;
        private readonly IChangeProcessor _changeProcessor;
        private readonly ISincConnector _connector;
        private readonly ILogger<DataPipeline> _logger;

        public string PipelineName { get; private set; }

        public DataPipeline(ICdcReader reader, IChangeProcessor changeProcessor, ISincConnector connector, string pipelineName, ILogger<DataPipeline> logger)
        {
            _reader = reader;
            _changeProcessor = changeProcessor;
            _connector = connector;
            _logger = logger;

            PipelineName = pipelineName;
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Pipeline '{PipelineName}' started.", PipelineName);

            try
            {
                await foreach (var changeDataMessage in _reader.ReadChangesAsync(cancellationToken))
                {
                    await _connector.SendChangesAsync(_changeProcessor.ProcessChanges(changeDataMessage));
                }
            } catch (OperationCanceledException)
            {
                _logger.LogWarning("Pipeline '{PipelineName}' was canceled.", PipelineName);
            } catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred in pipeline '{PipelineName}'.", PipelineName);
                throw;
            } finally
            {
                _logger.LogInformation("Pipeline '{PipelineName}' stopped.", PipelineName);
            }
        }
    }
}
