import 'dotenv/config';
import express from 'express';
import { Kafka, logLevel } from 'kafkajs';
import pkg from 'pg';
import { logger } from './utils/logger.js';
import {
    eventsProcessedTotal,
    eventsFailedTotal,
    dlqSentTotal,
    consumerRetriesTotal,
    metricsRegister
} from './utils/metrics.js';

const { Pool } = pkg;

const app = express();
const port = process.env.PORT || 3001;

app.get('/metrics', async (req, res) => {
    try {
        res.set('Content-Type', metricsRegister.contentType);
        res.end(await metricsRegister.metrics());
    } catch (ex) {
        res.status(500).end(ex.message);
    }
});

app.listen(port, () => {
    logger.info(`Payment metrics server listening on port ${port}`);
});

const brokers = process.env.KAFKA_BROKERS
    ? process.env.KAFKA_BROKERS.split(',')
    : ['localhost:19092'];

const groupId = process.env.GROUP_ID || 'payment-service-group';

const kafka = new Kafka({
    clientId: 'payment-service',
    brokers,
    logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId });
const producer = kafka.producer();

const pool = new Pool({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
});

pool.on('error', (err) => {
    logger.error({ error: err.message }, 'Unexpected error on idle database client');
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function withRetry(operation, maxAttempts = 5) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await operation();
            return; // Success
        } catch (error) {
            if (error.isPermanent) {
                throw error; // Don't retry permanent errors
            }
            if (attempt === maxAttempts) {
                throw new Error(`Operation failed after ${maxAttempts} attempts: ${error.message}`);
            }
            
            // Exponential backoff: 1s, 2s, 4s, 8s
            const baseMs = Math.pow(2, attempt - 1) * 1000;
            const jitterMs = Math.floor(Math.random() * (baseMs * 0.2)); // up to 20% jitter
            const waitMs = baseMs + jitterMs;
            
            consumerRetriesTotal.inc();
            logger.warn(`  ⚠️ [Payment] Transient failure simulated: ${error.message}. Retrying in ${waitMs}ms (Attempt ${attempt + 1} of ${maxAttempts})...`);
            await sleep(waitMs);
        }
    }
}

function parseEventPayload(rawValue) {
    if (!rawValue) {
        throw new Error('Message value is empty');
    }

    let parsed = JSON.parse(rawValue);

    // Case 1:
    // Kafka Connect / Debezium JSON converter wrapper
    //
    // {
    //   "schema": {...},
    //   "payload": "{\"orderId\":\"...\"}"
    // }
    if (
        parsed &&
        typeof parsed === 'object' &&
        'payload' in parsed
    ) {
        parsed = parsed.payload;
    }

    // Case 2:
    // Payload itself is JSON encoded as a string
    //
    // "{\"orderId\":\"...\"}"
    if (typeof parsed === 'string') {
        parsed = JSON.parse(parsed);
    }

    if (!parsed || typeof parsed !== 'object') {
        throw new Error('Invalid event payload');
    }

    return parsed;
}

async function run() {
    await consumer.connect();
    await producer.connect();

    logger.info(
        `✅ Payment Service connected to Redpanda (${brokers.join(',')})`
    );

    await consumer.subscribe({
        topic: 'ORDER.events',
        fromBeginning: true,
    });

    logger.info('Payment Service subscribed to ORDER.events');

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const eventId =
                message.headers?.id?.toString() || 'unknown';

            const rawPayload = message.value?.toString();

            try {
                const event = parseEventPayload(rawPayload);
                const correlationId = event.correlationId || 'unknown';

                logger.info({
                    correlationId,
                    topic,
                    partition,
                    offset: message.offset,
                    eventId
                }, '💳 PAYMENT SERVICE processing event');

                const orderId = event.orderId;
                
                // --- IDEMPOTENCY CHECK ---
                try {
                    await pool.query(
                        'INSERT INTO processed_events (event_id, service_name) VALUES ($1, $2)',
                        [eventId, 'payment-service']
                    );
                } catch (dbError) {
                    // 23505 is PostgreSQL unique_violation error code
                    if (dbError.code === '23505') {
                        logger.info({ correlationId, eventId }, `  ♻️ Event already processed, skipping.`);
                        return; // Exit early, do not process again
                    }
                    throw dbError; // Rethrow other database errors
                }
                
                // --- BUSINESS LOGIC WITH RETRY ---
                await withRetry(async () => {
                    // Random 30% chance to simulate a transient failure to demonstrate retries
                    if (Math.random() < 0.3) {
                        const err = new Error("Stripe API Timeout");
                        err.isPermanent = false;
                        throw err;
                    }

                    logger.info({
                        correlationId,
                        orderId,
                        userId: event.userId,
                        amount: event.amount
                    }, `  ✔️ Payment processed successfully for order ${orderId}`);
                });

                eventsProcessedTotal.inc();

            } catch (err) {
                eventsFailedTotal.inc();
                const correlationId = parseEventPayload(rawPayload)?.correlationId || 'unknown';

                logger.error({ correlationId, error: err.message, rawPayload },
                    `❌ [Payment Service] Failed to process message`
                );
                
                logger.warn({ correlationId }, `  ☠️ Message permanently failed. Sending to DLQ...`);
                try {
                    await producer.send({
                        topic: 'ORDER.dlq',
                        messages: [
                            {
                                key: eventId,
                                value: JSON.stringify({
                                    eventId,
                                    service: 'payment-service',
                                    error: err.message,
                                    stack: err.stack,
                                    originalPayload: rawPayload,
                                    failedAt: new Date().toISOString()
                                })
                            }
                        ]
                    });
                    dlqSentTotal.inc();
                    logger.info({ correlationId, eventId }, `  ✅ Successfully sent event to ORDER.dlq`);
                } catch (dlqErr) {
                    logger.fatal({ correlationId, error: dlqErr.message }, `  🔥 FATAL: Failed to send to DLQ`);
                }
            }
        },
    });
}

run().catch((error) => {
    logger.fatal({ error }, '❌ Payment Service failed');
    process.exit(1);
});