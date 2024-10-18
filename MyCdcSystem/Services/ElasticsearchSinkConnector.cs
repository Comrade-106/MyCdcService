using MyCdcSystem.Contracts;
using MyCdcSystem.Models.Configurations;
using MyCdcSystem.Models;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Clients.Elasticsearch.Mapping;
using MyCdcSystem.Banchmarcs;

namespace MyCdcSystem.Services
{
    public class ElasticsearchSinkConnector : ISincConnector
    {
        private readonly ElasticsearchClient _client;
        private readonly ILogger<ISincConnector> _logger;
        private readonly string _pipelineName;

        public ElasticsearchSinkConnector(ElasticsearchSinkConfiguration configuration, ILogger<ISincConnector> logger, string pipelineName)
        {
            var settings = new ElasticsearchClientSettings(new Uri(configuration.ElasticsearchUrl))
                .DefaultIndex("default_index");

            _client = new ElasticsearchClient(settings);
            _logger = logger;
            _pipelineName = pipelineName;
        }

        public async Task SendChangesAsync(ChangeDataMessage message)
        {
            if (message == null) return;

            var indexName = $"{message.Schema.ToLower()}.{message.Table.ToLower()}";

            _logger.LogInformation("Checking if index '{IndexName}' exists...", indexName);

            if (!await IndexExistsAsync(indexName))
            {
                _logger.LogInformation("Index '{IndexName}' does not exist. Creating index...", indexName);
                await CreateIndexAsync(indexName);
            }

            var document = CreateNewDocument(message);

            var bulkRequest = new BulkRequest
            {
                Operations = new List<IBulkOperation>
                {
                    new BulkIndexOperation<Dictionary<string, object>>(document)
                    {
                        Index = indexName,
                        Id = GetDocumentId(message.NewValues ?? message.OldValues)
                    }
                }
            };

            _logger.LogInformation("Sending change data to index '{IndexName}' in Elasticsearch.", indexName);
            var response = await _client.BulkAsync(bulkRequest);

            if (response.Errors)
            {
                SinkConnectorMetrics.SendErrorsTotal
                    .WithLabels(_pipelineName)
                    .Inc();

                _logger.LogError("Errors occurred during bulk operation in index '{IndexName}'.", indexName);
                foreach (var item in response.ItemsWithErrors)
                {
                    _logger.LogError("Error in operation {Operation}: {ErrorReason}", item.Operation, item.Error.Reason);
                }
            } else
            {
                SinkConnectorMetrics.MessagesSentTotal
                    .WithLabels(_pipelineName)
                    .Inc();
                                
                var endToEndLatency = (DateTime.UtcNow - message.TransactionCommitTimestamp).TotalSeconds;

                SinkConnectorMetrics.EndToEndLatencySeconds
                    .WithLabels(_pipelineName)
                    .Observe(endToEndLatency);
            }
        }

        private static Dictionary<string, object> CreateNewDocument(ChangeDataMessage message)
        {
            var document = new Dictionary<string, object>
            {
                { "operation", message.Operation },
                { "schema", message.Schema },
                { "table", message.Table },
                { "timestamp", message.Timestamp },
                { "transaction_xid", message.TransactionXid },
                { "commit_lsn", message.CommitLsn.ToString() },
                { "transaction_commit_timestamp", message.TransactionCommitTimestamp },
                { "new_values", message.NewValues },
                { "old_values", message.OldValues }
            };

            if (message.Operation.ToUpper() == "DELETE")
            {
                document["status"] = "deleted";
                document["deleted_at"] = DateTime.UtcNow;
            } else
            {
                document["status"] = "active";
            }

            return document;
        }

        private async Task<bool> IndexExistsAsync(string indexName)
        {
            var response = await _client.Indices.ExistsAsync(indexName);
            return response.Exists;
        }

        private async Task CreateIndexAsync(string indexName)
        {
            var createIndexResponse = await _client.Indices.CreateAsync(indexName, c => c
                .Mappings(m => m
                    .Properties(new Properties
                    {
                        { "operation", new KeywordProperty() },
                        { "schema", new KeywordProperty() },
                        { "table", new KeywordProperty() },
                        { "timestamp", new DateProperty() },
                        { "transaction_xid", new LongNumberProperty() },
                        { "commit_lsn", new KeywordProperty() }, // Используем Keyword, т.к. LSN — строка
                        { "transaction_commit_timestamp", new DateProperty() },
                        { "new_values", new ObjectProperty { Properties = new Properties() } },
                        { "old_values", new ObjectProperty { Properties = new Properties() } },
                        { "status", new KeywordProperty() }
                    })
                )
            );

            if (!createIndexResponse.IsValidResponse)
            {
                _logger.LogError("Failed to create index '{IndexName}': {Error}", indexName, createIndexResponse.ElasticsearchServerError);
            } else
            {
                _logger.LogInformation("Index '{IndexName}' created successfully.", indexName);
            }
        }

        private string GetDocumentId(Dictionary<string, object> values)
        {
            if (values != null && values.TryGetValue("id", out var idValue))
            {
                return idValue.ToString();
            } else
            {
                return Guid.NewGuid().ToString();
            }
        }
    }
}
