import promClient from 'prom-client';

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics({ prefix: 'notification_service_' });

export const eventsProcessedTotal = new promClient.Counter({
    name: 'notification_service_events_processed_total',
    help: 'Total number of events successfully processed'
});

export const eventsFailedTotal = new promClient.Counter({
    name: 'notification_service_events_failed_total',
    help: 'Total number of events failed'
});

export const dlqSentTotal = new promClient.Counter({
    name: 'notification_service_dlq_sent_total',
    help: 'Total number of events sent to DLQ'
});

export const consumerRetriesTotal = new promClient.Counter({
    name: 'notification_service_consumer_retries_total',
    help: 'Total number of retries performed'
});

export const metricsRegister = promClient.register;
