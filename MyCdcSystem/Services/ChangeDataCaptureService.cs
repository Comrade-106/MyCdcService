using MyCdcSystem.Core.Pipeline;

namespace MyCdcSystem.Services
{
    public class ChangeDataCaptureService
    {
        private readonly Dictionary<DataPipeline, CancellationTokenSource> _pipelines;
        private readonly ILogger<ChangeDataCaptureService> _logger;

        public ChangeDataCaptureService(ILogger<ChangeDataCaptureService> logger)
        {
            _pipelines = new Dictionary<DataPipeline, CancellationTokenSource>();
            _logger = logger;
        }

        public Task AddPipeline(DataPipeline pipeline)
        {
            if (_pipelines.Where(p => p.Key.PipelineName == pipeline.PipelineName).Any())
            {
                _logger.LogWarning("Pipeline with name '{PipelineName}' already exists and won't be added again.", pipeline.PipelineName);
                return null;
            }

            _logger.LogInformation("Adding pipeline '{PipelineName}'...", pipeline.PipelineName);

            var source = new CancellationTokenSource();
            _pipelines[pipeline] = source;

            try
            {
                _logger.LogInformation("Starting pipeline '{PipelineName}'...", pipeline.PipelineName);
                return pipeline.Start(source.Token);
            } catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start pipeline '{PipelineName}'", pipeline.PipelineName);
                throw;
            }
        }

        public void RemovePipeline(string pipelineName)
        {            
            _logger.LogWarning("Pipeline count: {Count}", _pipelines.Count);
            foreach (var pipeline in _pipelines)
            {
                _logger.LogWarning("Pipeline: {Name}", pipeline.Key.PipelineName);
            }

            var pipelineEntry = _pipelines.FirstOrDefault(p => p.Key.PipelineName == pipelineName);

            if (pipelineEntry.Key != null)
            {
                _logger.LogInformation("Stopping pipeline '{PipelineName}'...", pipelineName);
                pipelineEntry.Value.Cancel();  // Отмена через CancellationTokenSource

                _pipelines.Remove(pipelineEntry.Key);
                _logger.LogInformation("Pipeline '{PipelineName}' stopped and removed.", pipelineName);
            } else
            {
                _logger.LogWarning("Pipeline with name '{PipelineName}' not found.", pipelineName);
            }
        }
    }
}
