import dotenv from 'dotenv';
// Trigger nodemon restart
dotenv.config();

import express from 'express';
import pool from "./config/db.js";
import orderRoutes from './routes/order.routes.js';

const app = express();

app.use(express.json());
app.get("/health", async (req, res) => {
    try {
        const result = await pool.query("SELECT NOW()");
        res.json({
            status: "ok",
            service: "order-service",
            database: "connected",
            time: result.rows[0].now,
        });
    } catch (error) {
        console.error("Database health check failed:", error);

        res.status(500).json({
            status: "error",
            database: "disconnected",
        });

    }
});

app.use("/orders", orderRoutes);

const port = process.env.PORT || 3000;

app.listen(port, () => {
    console.log(`Order service running on port ${port}`);
});