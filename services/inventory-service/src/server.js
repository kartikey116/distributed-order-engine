import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import pkg from 'pg';
const { Pool } = pkg;

const brokers = process.env.KAFKA_BROKERS
    ? process.env.KAFKA_BROKERS.split(',')
    : ['localhost:19092'];

const groupId =
    process.env.GROUP_ID || 'inventory-service-group';

const kafka = new Kafka({
    clientId: 'inventory-service',
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
            
            console.log(`  ⚠️ [Inventory] Transient failure simulated: ${error.message}. Retrying in ${waitMs}ms (Attempt ${attempt + 1} of ${maxAttempts})...`);
            await sleep(waitMs);
        }
    }
}

function parseEventPayload(rawValue) {
    if (!rawValue) {
        throw new Error('Message value is empty');
    }

    // First JSON parse
    let parsed = JSON.parse(rawValue);

    // Handle Debezium/Kafka Connect wrapper:
    //
    // {
    //   schema: {...},
    //   payload: "{\"orderId\":\"...\"}"
    // }
    if (
        parsed &&
        typeof parsed === 'object' &&
        Object.prototype.hasOwnProperty.call(parsed, 'payload')
    ) {
        parsed = parsed.payload;
    }

    // Handle stringified JSON payload:
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
        `✅ Inventory Service connected to Redpanda (${brokers.join(',')})`
    );

    await consumer.subscribe({
        topic: 'ORDER.events',
        fromBeginning: true,
    });

    console.log(
        '📦 Inventory Service subscribed to ORDER.events'
    );

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            let eventId = message.headers?.id?.toString() || 'unknown';
            const rawPayload = message.value?.toString();

            try {
                const event = parseEventPayload(rawPayload);
                if (event.eventId) {
                    eventId = event.eventId;
                }

                const orderId = event.orderId;

                console.log('\n==============================');
                console.log('📦 INVENTORY SERVICE');
                console.log('==============================');

                console.log(`- Topic: ${topic}`);
                console.log(`- Partition: ${partition}`);
                console.log(`- Message Offset: ${message.offset}`);

                console.log(`- Event ID: ${eventId}`);
                console.log(`- Order ID: ${orderId}`);
                
                // --- IDEMPOTENCY CHECK ---
                try {
                    await pool.query(
                        'INSERT INTO processed_events (event_id, service_name) VALUES ($1, $2)',
                        [eventId, 'inventory-service']
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
                        const err = new Error("Database deadlock");
                        err.isPermanent = false;
                        throw err;
                    }

                    console.log(`- User ID: ${event.userId}`);
                    console.log(`- Amount: $${event.amount}`);
                    console.log(`- Status: ${event.status}`);

                    // Simulate inventory processing
                    console.log(
                        `  ✔️ Inventory reserved for order ${orderId}`
                    );
                });

            } catch (error) {
                console.error(
                    `❌ [Inventory Service] Failed to process message: ${error.message}`
                );

                console.error(
                    `Raw payload: ${rawPayload}`
                );
                
                console.log(`  ☠️ Message permanently failed. Sending to DLQ...`);
                try {
                    await producer.send({
                        topic: 'ORDER.dlq',
                        messages: [
                            {
                                key: eventId,
                                value: JSON.stringify({
                                    eventId,
                                    service: 'inventory-service',
                                    error: error.message,
                                    stack: error.stack,
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
    console.error(
        '❌ Inventory Service failed:',
        error
    );

    process.exit(1);
});