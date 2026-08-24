import dotenv from 'dotenv' ;
dotenv.config();

import express from 'express';
import orderRoutes from './routes/order.routes.js';

const app = express();

app.use(express.json());
app.get("/health",(req,res) => {
    res.json({
        status: "OK",
        service: "Order-Service"
    });
});

app.use("/orders", orderRoutes);

const port = process.env.PORT || 3000;

app.listen(port, () => {
    console.log(`Order service running on port ${port}`);
});