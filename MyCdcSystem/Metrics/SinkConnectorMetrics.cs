using Prometheus;

namespace MyCdcSystem.Banchmarcs
{
    public static class SinkConnectorMetrics
    {
        public static readonly Counter MessagesSentTotal = Metrics.CreateCounter(
            "sink_connector_messages_sent_total",
            "Общее количество изменений, отправленных в Elasticsearch.",
            new CounterConfiguration
            {
                LabelNames = new[] { "connector_id" }
            }
        );

        public static readonly Counter SendErrorsTotal = Metrics.CreateCounter(
            "sink_connector_send_errors_total",
            "Общее количество ошибок при отправке данных в Elasticsearch.",
            new CounterConfiguration
            {
                LabelNames = new[] { "connector_id" }
            }
        );

        public static readonly Histogram EndToEndLatencySeconds = Metrics.CreateHistogram(
            "sink_connector_end_to_end_latency_seconds",
            "Время задержки от коммита транзакции в базе данных до записи в Elasticsearch.",
            new HistogramConfiguration
            {
                LabelNames = new[] { "connector_id" }
            }
        );

        public static readonly Gauge GaugeEndToEndLatencySeconds = Metrics.CreateGauge(
            "sink_connector_end_to_end_latency_seconds",
            "Текущее значение задержки от коммита транзакции в базе данных до записи в Elasticsearch.",
            new GaugeConfiguration
            {
                LabelNames = new[] { "connector_id" }
            }
        );

    }
}
