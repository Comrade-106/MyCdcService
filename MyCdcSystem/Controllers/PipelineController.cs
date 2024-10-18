using Microsoft.AspNetCore.Mvc;
using MyCdcSystem.Contracts;
using MyCdcSystem.Core.Pipeline;
using MyCdcSystem.Core.StateSystem;
using MyCdcSystem.Models.Configurations;
using MyCdcSystem.Services;
using System.Text;
using System.Text.Json;

namespace MyCdcSystem.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PipelineController : ControllerBase
    {
        private readonly PipelineBuilder _pipelineBuilder;
        private readonly ChangeDataCaptureService _changeDataCaptureService;
        private readonly ILogger<PipelineController> _logger;

        public PipelineController(PipelineBuilder pipelineBuilder, ChangeDataCaptureService changeDataCaptureService, ILogger<PipelineController> logger)
        {
            _pipelineBuilder = pipelineBuilder;
            _changeDataCaptureService = changeDataCaptureService;
            _logger = logger;
        }

        [HttpPost]
        public async Task<IActionResult> CreatePipeline([FromBody] JsonDocument config)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            try
            {
                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(config.RootElement.GetRawText()));
                var configuration = new ConfigurationBuilder()
                    .AddJsonStream(stream)
                    .Build();

                var pipeline = _pipelineBuilder.BuildPipeline(configuration);
                _changeDataCaptureService.AddPipeline(pipeline);

                return Ok(new { message = $"Pipeline {pipeline.PipelineName} Started." });
            } catch (Exception ex)
            {
                _logger.LogError(ex, "Error while creating pipeline: {Error}", ex.Message);
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpDelete("{pipelineName}")]
        public IActionResult RemovePipeline(string pipelineName)
        {
            try
            {
                _changeDataCaptureService.RemovePipeline(pipelineName);
                return Ok(new { message = $"Pipeline '{pipelineName}' stopped and removed." });
            } catch (KeyNotFoundException)
            {
                return NotFound(new { error = $"Pipeline '{pipelineName}' not found." });
            } catch (Exception ex)
            {
                // Логирование ошибки
                return StatusCode(500, new { error = ex.Message });
            }
        }
    }
}
