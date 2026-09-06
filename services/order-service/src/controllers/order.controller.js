import { OrderService } from '../services/order.service.js';

export class OrderController {
    static async createOrder(req, res) {
        try {
            const { userId, amount } = req.body;

            if (!userId || amount === undefined || amount === null) {
                return res.status(400).json({
                    error: "Missing required fields: userId, amount"
                });
            }

            if (!Number.isFinite(Number(amount)) || Number(amount) <= 0) {
                return res.status(400).json({
                    error: "amount must be greater than 0"
                });
            }

            const order = await OrderService.createOrder(userId, amount, req.correlationId);

            res.status(201).json({
                message: "Order created successfully",
                order,
                correlationId: req.correlationId
            });
        } catch (error) {
            console.error("Failed to create order:", error);
            res.status(500).json({ error: "Internal server error" });
        }
    }
}
