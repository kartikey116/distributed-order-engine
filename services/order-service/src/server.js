import dotenv from 'dotenv';
// Trigger nodemon restart
dotenv.config();

import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import promClient from 'prom-client';
import pool from "./config/db.js";
import orderRoutes from './routes/order.routes.js';
import { logger } from './utils/logger.js';
import { httpRequestsTotal, metricsRegister } from './utils/metrics.js';

const app = express();

app.use(express.json());

// Correlation ID & Logging Middleware
app.use((req, res, next) => {
    req.correlationId = req.headers['x-correlation-id'] || uuidv4();
    res.setHeader('x-correlation-id', req.correlationId);

    logger.info({
        correlationId: req.correlationId,
        method: req.method,
        url: req.url
    }, 'Incoming request');

    const start = Date.now();
    res.on('finish', () => {
        const duration = Date.now() - start;
        logger.info({
            correlationId: req.correlationId,
            status: res.statusCode,
            durationMs: duration
        }, 'Request completed');

        httpRequestsTotal.inc({
            method: req.method,
            route: req.route ? req.route.path : req.path,
            status_code: res.statusCode
        });
    });
    next();
});

// Metrics endpoint
app.get('/metrics', async (req, res) => {
    try {
        res.set('Content-Type', metricsRegister.contentType);
        res.end(await metricsRegister.metrics());
    } catch (ex) {
        res.status(500).end(ex.message);
    }
});
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
    logger.info(`Order service running on port ${port}`);
});