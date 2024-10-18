using MyCdcSystem.Contracts;
using MyCdcSystem.Models.Configurations;
using MyCdcSystem.Models;
using System.Diagnostics;
using MyCdcSystem.Banchmarcs;

namespace MyCdcSystem.Services
{
    public class SimpleChangeProcessor : IChangeProcessor
    {
        private readonly SimpleChangeProcessorConfiguration _options;
        private readonly ILogger<IChangeProcessor> _logger;
        private readonly string _pipelineName;

        public SimpleChangeProcessor(SimpleChangeProcessorConfiguration options, ILogger<IChangeProcessor> logger, string pipelineName)
        {
            _options = options;
            _logger = logger;
            _pipelineName = pipelineName;
        }

        public ChangeDataMessage? ProcessChanges(ChangeDataMessage message)
        {
            var stopwatch = Stopwatch.StartNew();

            _logger.LogDebug("Processing message with operation '{Operation}', schema '{Schema}', and table '{Table}'.",
                        message.Operation, message.Schema, message.Table);

            ChangeProcessorMetrics.MessagesProcessedTotal.WithLabels(_pipelineName).Inc();

            if (!_options.AllowedOperations.Contains(message.Operation.ToUpper()))
            {
                _logger.LogInformation("Message filtered out by operation type: '{Operation}'", message.Operation);

                ChangeProcessorMetrics.MessagesFilteredTotal
                    .WithLabels(_pipelineName, "operation")
                    .Inc();

                return null;
            }

            var tableIdentifier = $"{message.Schema}.{message.Table}";

            if (_options.AllowedTables.Any() && !_options.AllowedTables.Contains(tableIdentifier))
            {
                _logger.LogInformation("Message filtered out by table identifier: '{TableIdentifier}'", tableIdentifier);

                ChangeProcessorMetrics.MessagesFilteredTotal
                    .WithLabels(_pipelineName, "table")
                    .Inc();

                return null;
            }

            if (_options.AllowedColumns.TryGetValue(tableIdentifier, out var allowedColumns) && allowedColumns.Any())
            {
                message.NewValues = FilterColumns(message.NewValues, allowedColumns);
                message.OldValues = FilterColumns(message.OldValues, allowedColumns);

                if ((message.NewValues == null || !message.NewValues.Any()) &&
                    (message.OldValues == null || !message.OldValues.Any()))
                {
                    _logger.LogInformation("Message filtered out after column filtering, no relevant data remaining.");

                    ChangeProcessorMetrics.MessagesFilteredTotal
                        .WithLabels(_pipelineName, "columns")
                        .Inc();

                    return null;
                }
            }

            stopwatch.Stop();
            ChangeProcessorMetrics.ProcessingDurationSeconds.Observe(stopwatch.Elapsed.TotalSeconds);

            return message;
        }

        private Dictionary<string, object>? FilterColumns(Dictionary<string, object>? values, HashSet<string> allowedColumns)
        {
            if (values == null)
                return null;

            var filteredValues = values
                .Where(kv => allowedColumns.Contains(kv.Key))
                .ToDictionary(kv => kv.Key, kv => kv.Value);

            return filteredValues.Any() ? filteredValues : null;
        }
    }
}
