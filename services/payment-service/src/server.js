import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import pkg from 'pg';
const { Pool } = pkg;

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
            
            console.log(`  ⚠️ [Payment] Transient failure simulated: ${error.message}. Retrying in ${waitMs}ms (Attempt ${attempt + 1} of ${maxAttempts})...`);
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

    console.log(
        `✅ Payment Service connected to Redpanda (${brokers.join(',')})`
    );

    await consumer.subscribe({
        topic: 'ORDER.events',
        fromBeginning: true,
    });

    console.log('Payment Service subscribed to ORDER.events');

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const eventId =
                message.headers?.id?.toString() || 'unknown';

            const rawPayload = message.value?.toString();

            try {
                const event = parseEventPayload(rawPayload);

                console.log('\n==============================');
                console.log('💳 PAYMENT SERVICE');
                console.log('==============================');

                console.log(`- Topic: ${topic}`);
                console.log(`- Partition: ${partition}`);
                console.log(`- Message Offset: ${message.offset}`);

                console.log(`- Event ID: ${eventId}`);
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
                        console.log(`  ♻️ Event ${eventId} already processed, skipping.`);
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

                    console.log(`- Order ID: ${orderId}`);
                    console.log(`- User ID: ${event.userId}`);
                    console.log(`- Amount: $${event.amount}`);
                    console.log(`- Status: ${event.status}`);

                    console.log(
                        `  ✔️ Payment processed successfully for order ${orderId}`
                    );
                });

            } catch (err) {
                console.error(
                    `❌ [Payment Service] Failed to process message: ${err.message}`
                );

                console.error(`Raw payload: ${rawPayload}`);
                
                console.log(`  ☠️ Message permanently failed. Sending to DLQ...`);
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
                    console.log(`  ✅ Successfully sent event ${eventId} to ORDER.dlq`);
                } catch (dlqErr) {
                    console.error(`  🔥 FATAL: Failed to send to DLQ: ${dlqErr.message}`);
                }
            }
        },
    });
}

run().catch((error) => {
    console.error('❌ Payment Service failed:', error);
    process.exit(1);
});