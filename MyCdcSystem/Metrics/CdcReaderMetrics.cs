using Prometheus;

namespace MyCdcSystem.Banchmarcs
{
    public static class CdcReaderMetrics
    {
        public static readonly Counter ChangesReadTotal = Metrics.CreateCounter(
            "cdc_reader_changes_total",
            "Общее количество прочитанных изменений из PostgreSQL.",
            new CounterConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );

        public static readonly Histogram ReadDurationSeconds = Metrics.CreateHistogram(
            "cdc_reader_read_duration_seconds",
            "Время, затраченное на чтение и обработку изменений.",
            new HistogramConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );

        public static readonly Counter ReadErrorsTotal = Metrics.CreateCounter(
            "cdc_reader_errors_total",
            "Количество ошибок при чтении изменений из PostgreSQL.",
            new CounterConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );

        public static readonly Gauge ProcessingLagSeconds = Metrics.CreateGauge(
            "cdc_reader_processing_lag_seconds",
            "Задержка обработки изменений в секундах.",
            new GaugeConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );

        public static readonly Gauge ConnectionStatus = Metrics.CreateGauge(
            "cdc_reader_connection_status",
            "Статус соединения с базой данных: 1 - активно, 0 - неактивно.",
            new GaugeConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );
    }

}
