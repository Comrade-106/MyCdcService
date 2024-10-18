using MyCdcSystem.Contracts;
using MyCdcSystem.Core.StateSystem;
using MyCdcSystem.Core;
using MyCdcSystem.Models.Configurations;
using MyCdcSystem.Models;
using Npgsql.Replication.PgOutput.Messages;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication;
using Npgsql;
using System.Runtime.CompilerServices;
using MyCdcSystem.Banchmarcs;
using System.Diagnostics;

namespace MyCdcSystem.Services
{
    public class PostgresCdcReader : ICdcReader
    {
        private readonly string _pipelineName;
        private string _connectionString;
        private string _slotName;
        private string _publicationName;
        private string _tableName;
        private readonly StateManager _stateManager;
        private readonly ILogger<ICdcReader> _logger;


        public PostgresCdcReader(PostgresConfiguration configuration, StateManager stateManager, ILogger<ICdcReader> logger, string pipelineName)
        {
            _connectionString = configuration.ConnectionString;
            _slotName = configuration.SlotName;
            _publicationName = configuration.PublicationName;
            _tableName = configuration.TableName;
            _stateManager = stateManager;
            _logger = logger;
            _pipelineName = pipelineName;
        }

        public async IAsyncEnumerable<ChangeDataMessage> ReadChangesAsync([EnumeratorCancellation] CancellationToken token)
        {
            _logger.LogInformation("Establishing connection to PostgreSQL database...");
            await using var connection = await CreateDatabaseConnectionAsync();

            _logger.LogInformation("Setting up replication for publication '{PublicationName}' and slot '{SlotName}'...", _publicationName, _slotName);
            await SetupReplication(connection);

            await using var replicationConnection = await CreateReplicationConnectionAsync();
            _logger.LogInformation("Replication connection established successfully.");

            var slot = new PgOutputReplicationSlot(_slotName);
            var factory = new ChangeDataMessageFactory();

            var changeDataMessage = new ChangeDataMessage();

            var stopwatch = Stopwatch.StartNew();

            await foreach (var message in replicationConnection.StartReplication(slot, new PgOutputReplicationOptions(_publicationName, 1), token))
            {
                if (message is BeginMessage)
                    changeDataMessage = new ChangeDataMessage();

                changeDataMessage = await factory.CreateChangeDataMessage(message, changeDataMessage);

                replicationConnection.SetReplicationStatus(message.WalEnd);

                if (message is CommitMessage)
                {
                    _stateManager.UpdateState(state =>
                    {
                        state.TransactionXid = changeDataMessage.TransactionXid;
                        state.Position = changeDataMessage.CommitLsn.ToString();
                        state.LastProcessedTimestamp = DateTime.UtcNow;
                    });

                    _logger.LogInformation("Transaction {TransactionXid} committed at position {CommitLsn}.", changeDataMessage.TransactionXid, changeDataMessage.CommitLsn);

                    CdcReaderMetrics.ChangesReadTotal
                        .WithLabels(_pipelineName)
                        .Inc();

                    var processingLag = (DateTime.UtcNow - changeDataMessage.TransactionCommitTimestamp).TotalSeconds;
                    CdcReaderMetrics.ProcessingLagSeconds
                        .WithLabels(_pipelineName)
                        .Set(processingLag);

                    yield return changeDataMessage;
                }
            }

            // Закрываем соединения
            await replicationConnection.DisposeAsync();
            await connection.CloseAsync();

            // Устанавливаем статус соединения в 0 (неактивно)
            CdcReaderMetrics.ConnectionStatus
                .WithLabels(_pipelineName)
                .Set(0);

            stopwatch.Stop();
            CdcReaderMetrics.ReadDurationSeconds
                .WithLabels(_pipelineName)
                .Observe(stopwatch.Elapsed.TotalSeconds);
        }

        private async Task<NpgsqlConnection> CreateDatabaseConnectionAsync()
        {
            try
            {
                var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();
                _logger.LogInformation("Connected to PostgreSQL database.");

                // Устанавливаем статус соединения в 1 (активно)
                CdcReaderMetrics.ConnectionStatus.Set(1);

                return connection;
            } catch (Exception ex)
            {
                _logger.LogError(ex, "Error connecting to PostgreSQL database.");

                // Увеличиваем счетчик ошибок
                CdcReaderMetrics.ReadErrorsTotal.Inc();

                // Устанавливаем статус соединения в 0 (неактивно)
                CdcReaderMetrics.ConnectionStatus.Set(0);

                throw;
            }
        }

        private async Task<LogicalReplicationConnection> CreateReplicationConnectionAsync()
        {
            var replicationConnection = new LogicalReplicationConnection(_connectionString);
            await replicationConnection.Open();
            return replicationConnection;
        }

        private async Task SetupReplication(NpgsqlConnection connection)
        {
            await CreatePublicationIfNotExists(connection, _publicationName, _tableName);
            await CreateReplicationSlotIfNotExists(connection, _slotName);
            await AlterTablePublication(connection, _tableName);
        }

        private async Task CreateReplicationSlotIfNotExists(NpgsqlConnection conn, string slotName)
        {
            var checkSlotQuery = $"SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{slotName}';";
            var createSlotQuery = $"SELECT * FROM pg_create_logical_replication_slot('{slotName}', 'pgoutput');";

            _logger.LogInformation("Checking if replication slot '{SlotName}' exists...", slotName);

            await using var cmd = new NpgsqlCommand(checkSlotQuery, conn);
            var result = await cmd.ExecuteScalarAsync();

            if (result == null)
            {
                _logger.LogInformation("Replication slot '{SlotName}' does not exist. Creating slot...", slotName);

                await using var createCmd = new NpgsqlCommand(createSlotQuery, conn);
                await createCmd.ExecuteNonQueryAsync();

                _logger.LogInformation("Replication slot '{SlotName}' created successfully.", slotName);
            } else
            {
                _logger.LogInformation("Replication slot '{SlotName}' already exists.", slotName);
            }
        }

        private async Task CreatePublicationIfNotExists(NpgsqlConnection conn, string publicationName, string tableName)
        {
            // Проверка наличия публикации
            var checkPublicationQuery = $"SELECT COUNT(1) FROM pg_publication WHERE pubname = '{publicationName}';";
            await using var checkCmd = new NpgsqlCommand(checkPublicationQuery, conn);
            var publicationExists = (long)await checkCmd.ExecuteScalarAsync() > 0;

            if (!publicationExists)
            {
                _logger.LogInformation("Publication '{PublicationName}' does not exist. Creating publication for table '{TableName}'...", publicationName, tableName);

                // Создание публикации
                var createPublicationQuery = $"CREATE PUBLICATION {publicationName} FOR TABLE {tableName};";
                await using var createCmd = new NpgsqlCommand(createPublicationQuery, conn);
                await createCmd.ExecuteNonQueryAsync();

                _logger.LogInformation("Publication '{PublicationName}' created successfully.", publicationName);
            } else
            {
                _logger.LogInformation("Publication '{PublicationName}' already exists.", publicationName);
            }
        }

        private async Task AlterTablePublication(NpgsqlConnection conn, string tableName)
        {
            _logger.LogInformation("Setting replica identity for table '{TableName}' to FULL...", tableName);

            var alterTableQuery = $"ALTER TABLE {tableName} REPLICA IDENTITY FULL;";

            // Проверяем, существует ли слот.
            await using var cmd = new NpgsqlCommand(alterTableQuery, conn);
            var result = await cmd.ExecuteScalarAsync();

            _logger.LogInformation("Replica identity for table '{TableName}' set to FULL.", tableName);
        }
    }
}
