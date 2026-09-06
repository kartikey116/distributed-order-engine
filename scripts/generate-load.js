const http = require('http');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function generateLoad() {
    let orderCount = 0;
    
    console.log("Starting load generation...");
    console.log("Sending 1 order every 500ms.");
    console.log("Press Ctrl+C to stop.\n");

    setInterval(() => {
        const payload = JSON.stringify({
            userId: "550e8400-e29b-41d4-a716-446655440000",
            amount: Math.floor(Math.random() * 1000) + 10
        });

        const options = {
            hostname: 'localhost',
            port: 3000,
            path: '/orders',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': payload.length
            }
        };

        const req = http.request(options, res => {
            orderCount++;
            process.stdout.write(`\rOrders created: ${orderCount} `);
        });

        req.on('error', error => {
            console.error(`\nFailed to create order: ${error.message}`);
        });

        req.write(payload);
        req.end();
    }, 500); // 1 order every 500ms
}

generateLoad();
