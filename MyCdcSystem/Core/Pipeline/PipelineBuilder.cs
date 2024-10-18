using MyCdcSystem.Contracts;
using MyCdcSystem.Models.Configurations;
using MyCdcSystem.Services;
using MyCdcSystem.Core.StateSystem;
using Newtonsoft.Json;
using System.Reflection;

namespace MyCdcSystem.Core.Pipeline
{
    public class PipelineBuilder
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly StateManager _stateManager;

        public PipelineBuilder(IServiceProvider serviceProvider, StateManager stateManager)
        {
            _serviceProvider = serviceProvider;
            _stateManager = stateManager;
        }

        public DataPipeline BuildPipeline(IConfiguration configuration)
        {
            var pipelineName = configuration.GetValue<string>("PipelineName");
            var logger = (ILogger<DataPipeline>)_serviceProvider.GetService(typeof(ILogger<>).MakeGenericType(typeof(DataPipeline)));

            return new DataPipeline(
                CreateCdcReader(configuration.GetSection("Source").Get<Dictionary<string, object>>(), pipelineName),
                CreateChangeProcessor(configuration.GetSection("Processor"), pipelineName),
                CreateSincConnector(configuration.GetSection("Sink").Get<Dictionary<string, object>>(), pipelineName),
                pipelineName,
                logger);
        }

        public ICdcReader CreateCdcReader(Dictionary<string, object> config, string pipelineName)
        {
            if (!config.TryGetValue("ReaderType", out var readerTypeObj))
                throw new ArgumentException("Не указан тип ридера в конфигурации.");

            var readerTypeName = readerTypeObj.ToString();
            config.Remove("ReaderType");

            Type? readerType = GetTypeFromName(readerTypeName);

            if (readerType == null)
                throw new ArgumentException($"Неизвестный тип ридера: {readerTypeName}");

            var configTypeName = readerTypeName.Replace("CdcReader", "Configuration");

            var configType = GetTypeFromName(configTypeName);

            if (configType == null)
                throw new ArgumentException($"Неизвестный тип конфигурации для ридера: {readerTypeName}");

            var configuration = JsonConvert.DeserializeObject(JsonConvert.SerializeObject(config), configType);

            var loggerType = typeof(ILogger<>).MakeGenericType(readerType);
            var logger = _serviceProvider.GetService(loggerType);

            return (ICdcReader)Activator.CreateInstance(readerType, configuration, _stateManager, logger, pipelineName);
        }

        public IChangeProcessor CreateChangeProcessor(IConfigurationSection config, string pipelineName)
        {
            var processorTypeName = config.GetValue<string>("ProcessorType");
            if (string.IsNullOrEmpty(processorTypeName))
                throw new ArgumentException("Не указан тип процессора в конфигурации.");

            Type? processorType = GetTypeFromName(processorTypeName);
            if (processorType == null)
                throw new ArgumentException($"Неизвестный тип процессора: {processorTypeName}");

            var configTypeName = processorTypeName + "Configuration";
            Type? configType = GetTypeFromName(configTypeName);
            if (configType == null)
                throw new ArgumentException($"Неизвестный тип конфигурации для процессора: {processorTypeName}");

            var configuration = config.Get(configType);

            var loggerType = typeof(ILogger<>).MakeGenericType(processorType);
            var logger = _serviceProvider.GetService(loggerType);

            return (IChangeProcessor)Activator.CreateInstance(processorType, configuration, logger, pipelineName);
        }

        public ISincConnector CreateSincConnector(Dictionary<string, object> config, string pipelineName)
        {
            if (!config.TryGetValue("ConnectorType", out var connectorNameObj))
                throw new ArgumentException("Не указан тип соединителя в конфигурации.");

            var connectorTypeName = connectorNameObj.ToString();
            config.Remove("ConnectorType");

            Type? connectorType = GetTypeFromName(connectorTypeName);

            if (connectorType == null)
                throw new ArgumentException($"Неизвестный тип коннектора: {connectorTypeName}");

            var configTypeName = connectorTypeName.Replace("Connector", "Configuration");

            var configType = GetTypeFromName(configTypeName);

            if (configType == null)
                throw new ArgumentException($"Неизвестный тип конфигурации {configTypeName} для соединителя: {connectorTypeName}");

            var configuration = JsonConvert.DeserializeObject(JsonConvert.SerializeObject(config), configType);

            var logger = _serviceProvider.GetService(typeof(ILogger<>).MakeGenericType(connectorType));

            return (ISincConnector)Activator.CreateInstance(connectorType, configuration, logger, pipelineName);
        }

        private static Type? GetTypeFromName(string? typeName)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var type = assembly.GetTypes()
                .FirstOrDefault(t => t.Name.Equals(typeName, StringComparison.OrdinalIgnoreCase));

            return type;
        }
    }
}
