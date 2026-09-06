import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import pkg from 'pg';
const { Pool } = pkg;

const brokers = process.env.KAFKA_BROKERS
    ? process.env.KAFKA_BROKERS.split(',')
    : ['localhost:19092'];

const groupId =
    process.env.GROUP_ID || 'notification-service-group';

const kafka = new Kafka({
    clientId: 'notification-service',
    brokers,
    logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId });

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
            
            console.log(`  ⚠️ [Notification] Transient failure simulated: ${error.message}. Retrying in ${waitMs}ms (Attempt ${attempt + 1} of ${maxAttempts})...`);
            await sleep(waitMs);
        }
    }
}

function parseEventPayload(rawValue) {
    if (!rawValue) {
        throw new Error('Message value is empty');
    }

    let parsed = JSON.parse(rawValue);

    // Debezium/Kafka Connect wrapper
    if (
        parsed &&
        typeof parsed === 'object' &&
        Object.prototype.hasOwnProperty.call(parsed, 'payload')
    ) {
        parsed = parsed.payload;
    }

    // Stringified JSON payload
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

    console.log(
        `✅ Notification Service connected to Redpanda (${brokers.join(',')})`
    );

    await consumer.subscribe({
        topic: 'ORDER.events',
        fromBeginning: true,
    });

    console.log(
        '📧 Notification Service subscribed to ORDER.events'
    );

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const rawPayload = message.value?.toString();

            try {
                const event = parseEventPayload(rawPayload);

                const eventId =
                    message.headers?.id?.toString() ||
                    event.eventId ||
                    'unknown';

                // IMPORTANT:
                // Extract these values exactly once.
                const orderId = String(event.orderId);
                const userId = String(event.userId);

                // Validate the Order ID
                if (!orderId || orderId === 'undefined') {
                    throw new Error('Missing orderId in event');
                }

                console.log('\n==============================');
                console.log('📧 NOTIFICATION SERVICE');
                console.log('==============================');

                console.log(`- Topic: ${topic}`);
                console.log(`- Partition: ${partition}`);
                console.log(`- Message Offset: ${message.offset}`);

                console.log(`- Event ID: ${eventId}`);
                
                // --- IDEMPOTENCY CHECK ---
                try {
                    await pool.query(
                        'INSERT INTO processed_events (event_id, service_name) VALUES ($1, $2)',
                        [eventId, 'notification-service']
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
                        const err = new Error("SendGrid API Rate Limit");
                        err.isPermanent = false;
                        throw err;
                    }

                    console.log(`- Order ID: ${orderId}`);
                    console.log(`- User ID: ${userId}`);
                    console.log(`- Status: ${event.status}`);

                    console.log(
                        `  📧 Email notification sent to user ${userId} for order ${orderId}`
                    );
                });

            } catch (error) {
                console.error(
                    `❌ [Notification Service] Failed to process message: ${error.message}`
                );

                console.error(
                    `Raw payload: ${rawPayload}`
                );
            }
        },
    });
}

run().catch((error) => {
    console.error(
        '❌ Notification Service failed:',
        error
    );

    process.exit(1);
});