import promClient from 'prom-client';

const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics({ prefix: 'order_service_' });

export const httpRequestsTotal = new promClient.Counter({
    name: 'order_service_http_requests_total',
    help: 'Total number of HTTP requests',
    labelNames: ['method', 'route', 'status_code']
});

export const ordersCreatedTotal = new promClient.Counter({
    name: 'order_service_orders_created_total',
    help: 'Total number of orders created'
});

export const metricsRegister = promClient.register;
