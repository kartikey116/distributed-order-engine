/**
 * Debezium Watchdog
 * Polls the Kafka Connect REST API every 5 seconds.
 * If any connector task is in FAILED state, it automatically restarts it.
 */

const CONNECT_URL = 'http://localhost:8083';
const CONNECTOR_NAME = 'outbox-connector';
const POLL_INTERVAL_MS = 5000;

async function checkAndHeal() {
    try {
        const res = await fetch(`${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status`);
        if (!res.ok) {
            console.log(`[${ts()}] WARNING Could not reach Kafka Connect (HTTP ${res.status}). Will retry...`);
            return;
        }

        const status = await res.json();
        const failedTasks = status.tasks.filter(t => t.state === 'FAILED');

        if (failedTasks.length === 0) {
            const taskStates = status.tasks.map(t => t.state).join(', ');
            console.log(`[${ts()}] OK Debezium healthy -- Connector: ${status.connector.state} | Tasks: ${taskStates}`);
            return;
        }

        console.log(`[${ts()}] ALERT DETECTED ${failedTasks.length} FAILED task(s)! Auto-restarting...`);

        for (const task of failedTasks) {
            const restartRes = await fetch(
                `${CONNECT_URL}/connectors/${CONNECTOR_NAME}/tasks/${task.id}/restart`,
                { method: 'POST' }
            );
            if (restartRes.status === 204 || restartRes.status === 200) {
                console.log(`[${ts()}] Task ${task.id} restart request sent successfully.`);
            } else {
                console.log(`[${ts()}] FAILED to restart task ${task.id}: HTTP ${restartRes.status}`);
            }
        }

        await new Promise(r => setTimeout(r, 3000));
        const verifyRes = await fetch(`${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status`);
        const verified = await verifyRes.json();
        const newStates = verified.tasks.map(t => `Task ${t.id}: ${t.state}`).join(', ');
        console.log(`[${ts()}] Post-restart status -- ${newStates}`);

    } catch (err) {
        console.log(`[${ts()}] Watchdog error: ${err.message}. Will retry...`);
    }
}

function ts() {
    return new Date().toLocaleTimeString('en-IN', { hour12: false });
}

console.log(`[${ts()}] Debezium Watchdog started. Polling every ${POLL_INTERVAL_MS / 1000}s...`);
checkAndHeal();
setInterval(checkAndHeal, POLL_INTERVAL_MS);
