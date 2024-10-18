using Prometheus;

namespace MyCdcSystem.Banchmarcs
{
    public static class ChangeProcessorMetrics
    {
        public static readonly Counter MessagesProcessedTotal = Metrics.CreateCounter(
            "change_processor_messages_processed_total",
            "Общее количество сообщений, обработанных ChangeProcessor.",
            new CounterConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );

        public static readonly Counter MessagesFilteredTotal = Metrics.CreateCounter(
            "change_processor_messages_filtered_total",
            "Общее количество сообщений, отфильтрованных ChangeProcessor.",
            new CounterConfiguration
            {
                LabelNames = new[] { "pipeline_name", "reason" }
            }
        );

        public static readonly Histogram ProcessingDurationSeconds = Metrics.CreateHistogram(
            "change_processor_processing_duration_seconds",
            "Время, затраченное на обработку сообщений ChangeProcessor.",
            new HistogramConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );

        public static readonly Counter ProcessingErrorsTotal = Metrics.CreateCounter(
            "change_processor_errors_total",
            "Общее количество ошибок, возникших в ChangeProcessor.",
            new CounterConfiguration
            {
                LabelNames = new[] { "pipeline_name" }
            }
        );
    }

}
