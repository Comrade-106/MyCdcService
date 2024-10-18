using MyCdcSystem.Models;
using Npgsql.Replication.PgOutput.Messages;

namespace MyCdcSystem.Core
{
    public class ChangeDataMessageFactory
    {
        public async Task<ChangeDataMessage> CreateChangeDataMessage(PgOutputReplicationMessage message, ChangeDataMessage changeDataMessage)
        {
            return await CreateChangeDataMessage((dynamic)message, changeDataMessage);
        }

        private async Task<ChangeDataMessage> CreateChangeDataMessage(InsertMessage message, ChangeDataMessage changeDataMessage)
        {

            changeDataMessage.Operation = "INSERT";
            changeDataMessage.Schema = message.Relation.Namespace;
            changeDataMessage.Table = message.Relation.RelationName;
            changeDataMessage.NewValues = new Dictionary<string, object>();

            var columns = message.Relation.Columns;
            int columnIndex = 0;
            await foreach (var column in message.NewRow)
            {
                var columnName = columns[columnIndex].ColumnName;

                changeDataMessage.NewValues[columnName] = await column.Get();
                columnIndex++;
            }

            return changeDataMessage;
        }

        private async Task<ChangeDataMessage> CreateChangeDataMessage(FullDeleteMessage message, ChangeDataMessage changeDataMessage)
        {
            changeDataMessage.Operation = "DELETE";
            changeDataMessage.Schema = message.Relation.Namespace;
            changeDataMessage.Table = message.Relation.RelationName;
            changeDataMessage.OldValues = new Dictionary<string, object>();

            var columns = message.Relation.Columns;
            int columnIndex = 0;
            await foreach (var column in message.OldRow)
            {
                var columnName = columns[columnIndex].ColumnName;

                changeDataMessage.OldValues[columnName] = await column.Get();
                columnIndex++;
            }

            return changeDataMessage;
        }

        private async Task<ChangeDataMessage> CreateChangeDataMessage(FullUpdateMessage message, ChangeDataMessage changeDataMessage)
        {
            changeDataMessage.Operation = "UPDATE";
            changeDataMessage.Schema = message.Relation.Namespace;
            changeDataMessage.Table = message.Relation.RelationName;
            changeDataMessage.OldValues = new Dictionary<string, object>();
            changeDataMessage.NewValues = new Dictionary<string, object>();

            var columns = message.Relation.Columns;

            // Старые значения
            int oldColumnIndex = 0;
            await foreach (var column in message.OldRow)
            {
                var columnName = columns[oldColumnIndex].ColumnName;

                changeDataMessage.OldValues[columnName] = await column.Get();
                oldColumnIndex++;
            }

            // Новые значения
            int newColumnIndex = 0;
            await foreach (var column in message.NewRow)
            {
                var columnName = columns[newColumnIndex].ColumnName;

                changeDataMessage.NewValues[columnName] = await column.Get();
                newColumnIndex++;
            }

            return changeDataMessage;
        }

        private async Task<ChangeDataMessage> CreateChangeDataMessage(TruncateMessage message, ChangeDataMessage changeDataMessage)
        {
            changeDataMessage.Operation = "DELETE";
            changeDataMessage.Schema = message.Relations.Select(p => p.Namespace).ToList().ToString();
            changeDataMessage.Table = message.Relations.Select(p => p.RelationName).ToList().ToString();

            return changeDataMessage;
        }


        private async Task<ChangeDataMessage> CreateChangeDataMessage(BeginMessage message, ChangeDataMessage changeDataMessage)
        {
            changeDataMessage.TransactionXid = message.TransactionXid;
            changeDataMessage.TransactionCommitTimestamp = message.TransactionCommitTimestamp;
            changeDataMessage.Timestamp = DateTime.UtcNow;

            return changeDataMessage;
        }

        private async Task<ChangeDataMessage> CreateChangeDataMessage(RelationMessage message, ChangeDataMessage changeDataMessage)
        {
            return changeDataMessage;
        }

        private async Task<ChangeDataMessage> CreateChangeDataMessage(CommitMessage message, ChangeDataMessage changeDataMessage)
        {
            changeDataMessage.CommitLsn = message.CommitLsn;

            return changeDataMessage;
        }
    }
}
