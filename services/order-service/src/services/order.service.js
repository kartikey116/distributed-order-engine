import { v4 as uuidv4 } from 'uuid';
import pool from '../config/db.js';

export class OrderService {
    /**
     * Creates an order and its corresponding outbox event
     * inside a single PostgreSQL transaction.
     *
     * This guarantees that the business state and the event
     * record are committed atomically.
     */
    static async createOrder(userId, amount) {
        const client = await pool.connect();

        const orderId = uuidv4();
        const eventId = uuidv4();

        try {
            await client.query('BEGIN');

            // 1. Create the order
            const orderQuery = `
                INSERT INTO orders (
                    id,
                    user_id,
                    amount,
                    status
                )
                VALUES ($1, $2, $3, $4)
                RETURNING *;
            `;

            const orderResult = await client.query(orderQuery, [
                orderId,
                userId,
                amount,
                'PENDING'
            ]);

            const createdOrder = orderResult.rows[0];

            // 2. Create the event payload
            const eventPayload = {
                eventId,
                orderId: createdOrder.id,
                userId: createdOrder.user_id,
                amount: createdOrder.amount,
                status: createdOrder.status,
                timestamp: createdOrder.created_at
            };

            // 3. Store the event in the outbox
            const outboxQuery = `
                INSERT INTO outbox_events (
                    id,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload
                )
                VALUES ($1, $2, $3, $4, $5);
            `;

            await client.query(outboxQuery, [
                eventId,
                'ORDER',
                orderId,
                'ORDER_CREATED',
                JSON.stringify(eventPayload)
            ]);

            // 4. Atomically commit order + event
            await client.query('COMMIT');

            return createdOrder;

        } catch (error) {
            await client.query('ROLLBACK');
            throw error;

        } finally {
            client.release();
        }
    }
}