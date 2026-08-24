import express from 'express';
const router = express.Router();

router.get("/", (req, res) => {
    res.json({
        message: "Get orders",
    });
});

router.post("/", (req, res) => {
    res.json({
        message: "Create order",
    });
});

export default router;