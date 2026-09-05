                    PROJECT BUILD

1. Docker Infrastructure                 ✅
        │
        ▼
2. Node.js + Express Order Service       ← NOW
        │
        ▼
3. PostgreSQL Connection
        │
        ▼
4. POST /orders
        │
        ▼
5. PostgreSQL Transaction
        │
        ├── orders
        └── outbox_events
        │
        ▼
6. Test Atomicity
        │
        ▼
7. Debezium Connector
        │
        ▼
8. PostgreSQL WAL → Debezium
        │
        ▼
9. Debezium → Redpanda
        │
        ▼
10. Kafka Consumer
        │
        ▼
11. Payment Service
        │
        ▼
12. Redis Idempotency
        │
        ▼
13. Database Idempotency
        │
        ▼
14. Retry + Exponential Backoff
        │
        ▼
15. DLQ
        │
        ▼
16. Failure Testing
        │
        ▼
17. Load Testing
        │
        ▼
18. Kubernetes


Overall Roadmap
                         ┌─────────────────────┐
                         │     Node.js API     │
                         │    Order Service    │
                         └──────────┬──────────┘
                                    │
                                    │ 1. Create Order
                                    ▼
                         ┌─────────────────────┐
                         │    PostgreSQL       │
                         │                     │
                         │  ┌───────────────┐  │
                         │  │    orders     │  │
                         │  └───────────────┘  │
                         │          +           │
                         │  ┌───────────────┐  │
                         │  │outbox_events  │  │
                         │  └───────────────┘  │
                         └──────────┬──────────┘
                                    │
                                    │ WAL
                                    ▼
                         ┌─────────────────────┐
                         │      Debezium       │
                         │       CDC           │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     Redpanda        │
                         │    Kafka Topics     │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
              Payment Service  Inventory       Notification
                    │
                    ▼
                  Redis
             Idempotency/Dedupe




             # Master Prompt — Distributed Transactional Outbox & AI-Assisted Order Processing Platform

Act as a **Senior Backend Engineer, Distributed Systems Architect, and Research-Level Software Engineer** mentoring me while I build a portfolio-grade distributed systems project.

I am building the following project:

# Project Name

**Distributed Transactional Outbox & Event-Driven Order Processing Engine**

Later, this will be extended into:

**AI-Assisted Self-Healing Distributed Order Processing Platform**

---

# 1. Problem We Are Solving

The core problem is the **Dual-Write Problem** in distributed systems.

Suppose an Order Service needs to:

1. Save an order in PostgreSQL.
2. Publish an `ORDER_CREATED` event to Kafka/Redpanda.

A naive implementation might do:

```text
POST /orders
     │
     ├── INSERT order → PostgreSQL
     │
     └── Publish event → Kafka
```

This creates a consistency problem.

For example:

```text
INSERT order → SUCCESS
Publish Kafka event → FAILURE
```

Now PostgreSQL says:

```text
Order exists
```

but downstream services never receive:

```text
ORDER_CREATED
```

The reverse can also happen:

```text
Publish Kafka event → SUCCESS
INSERT order → FAILURE
```

Now downstream services believe an order exists when the database does not contain it.

This is the **dual-write problem**.

---

# 2. Core Solution

We solve this using the **Transactional Outbox Pattern**.

Instead of directly writing to PostgreSQL and Kafka independently:

```text
Order Service
     │
     ├── PostgreSQL
     └── Kafka
```

we perform both database writes inside the **same PostgreSQL transaction**:

```text
BEGIN

INSERT INTO orders (...)

INSERT INTO outbox_events (...)

COMMIT
```

Therefore:

```text
Order + Event
     │
     ▼
Same PostgreSQL Transaction
```

Either both are committed:

```text
orders       → SUCCESS
outbox_event → SUCCESS
```

or both are rolled back:

```text
orders       → ROLLBACK
outbox_event → ROLLBACK
```

Then **Debezium CDC** reads PostgreSQL's WAL and publishes the outbox event to **Redpanda/Kafka**.

Architecture:

```text
Client
  │
  ▼
Node.js Order Service
  │
  ▼
PostgreSQL Transaction
  │
  ├── orders
  │
  └── outbox_events
          │
          ▼
       PostgreSQL WAL
          │
          ▼
       Debezium CDC
          │
          ▼
      Redpanda/Kafka
          │
          ├── Payment Service
          ├── Inventory Service
          └── Notification Service
```

The goal is **at-least-once event delivery with reliable persistence**, while accepting that consumers must be idempotent because duplicate delivery is possible.

---

# 3. Why This Is a Distributed Systems Project

The project should demonstrate real distributed-systems concepts:

- Distributed transactions
- Transactional Outbox
- CDC
- PostgreSQL WAL
- Apache Kafka / Redpanda
- Event-driven architecture
- At-least-once delivery
- Idempotent consumers
- Idempotency keys
- Deduplication
- Redis
- Retry mechanisms
- Exponential backoff
- Dead Letter Queue
- Failure recovery
- Observability
- Logs
- Metrics
- Distributed tracing
- Fault injection
- Eventual consistency
- Backpressure
- Consumer failures
- Network partitions
- Service failures
- Data consistency

The project should not be treated as a simple CRUD application.

The objective is to demonstrate **production-style distributed backend engineering**.

---

# 4. Technology Stack

## Backend

**Node.js + Express.js**

Use JavaScript rather than TypeScript unless I explicitly ask for TypeScript.

Use modern JavaScript/ES Modules consistently.

---

## Database

**PostgreSQL 17**

Used for:

- Orders
- Transactional Outbox
- Idempotency records
- Business state
- Event metadata

Important PostgreSQL configuration:

```yaml
wal_level=logical
max_wal_senders=10
max_replication_slots=10
```

Explain these when relevant.

### WAL

PostgreSQL Write-Ahead Log records database changes before they are permanently applied.

Our CDC flow is:

```text
PostgreSQL
    │
    ▼
WAL
    │
    ▼
Debezium
    │
    ▼
Redpanda
```

### Logical Replication

We use:

```text
wal_level=logical
```

because Debezium needs logical change information for CDC.

---

# 5. Message Broker

Use:

**Redpanda**

Redpanda provides Kafka-compatible APIs.

It will be used for:

- Event streaming
- Order events
- Consumer groups
- Retry topics
- Dead Letter Queue
- Event-driven communication

Our local setup uses:

```text
Host → localhost:19092
Docker internal → redpanda:9092
```

The Redpanda listeners are:

```yaml
--kafka-addr
internal://0.0.0.0:9092,external://0.0.0.0:19092

--advertise-kafka-addr
internal://redpanda:9092,external://localhost:19092
```

Explain the difference between internal and external Kafka listeners whenever relevant.

---

# 6. CDC

Use:

**Debezium**

Debezium runs through Kafka Connect.

Current image:

```text
quay.io/debezium/connect:3.0.0.Final
```

Debezium connects to:

```text
redpanda:9092
```

and PostgreSQL through the Docker network.

The intended flow:

```text
PostgreSQL
    │
    │ WAL
    ▼
Debezium
    │
    │ CDC
    ▼
Redpanda
```

We should configure Debezium so that changes from the transactional outbox are published as events.

---

# 7. Redis

Use:

**Redis 8**

Redis will be used for:

- Fast idempotency checks
- Duplicate event detection
- Short-lived deduplication
- Sliding-window deduplication
- Rate limiting if useful
- Fast operational state

However, Redis must **not** be treated as the ultimate source of truth for critical business state.

PostgreSQL remains authoritative for durable business state.

---

# 8. Docker

Use Docker from the beginning.

Current infrastructure:

```text
docker-compose.yml

PostgreSQL 17
Redpanda
Redis 8
Debezium Kafka Connect
```

Important current PostgreSQL port mapping:

```yaml
ports:
  - "5433:5432"
```

because the Windows host already has PostgreSQL using port `5432`.

Therefore:

### Node.js running on Windows

```text
127.0.0.1:5433
```

### PostgreSQL inside Docker

```text
postgres:5432
```

### Debezium inside Docker

Use the Docker service name:

```text
postgres:5432
```

Do NOT use `127.0.0.1:5433` from inside the Debezium container.

---

# 9. Current Database Schema

We currently have:

## orders

```sql
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## outbox_events

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
```

Indexes:

```sql
CREATE INDEX idx_outbox_events_created_at
ON outbox_events(created_at);

CREATE INDEX idx_outbox_events_status
ON outbox_events(status);
```

Current database has successfully created:

```text
orders
outbox_events
```

---

# 10. Current Project Structure

Current direction:

```text
distributed-order-engine/
│
├── services/
│   └── order-service/
│       │
│       ├── src/
│       │   ├── config/
│       │   │   └── db.js
│       │   │
│       │   ├── controllers/
│       │   │   └── order.controller.js
│       │   │
│       │   ├── routes/
│       │   │   └── order.routes.js
│       │   │
│       │   ├── services/
│       │   │   └── order.service.js
│       │   │
│       │   └── server.js
│       │
│       ├── package.json
│       ├── package-lock.json
│       ├── .env
│       └── .gitignore
│
├── infrastructure/
│   └── postgres/
│       └── init/
│           └── 001-init.sql
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

# 11. Current Environment

Node.js Order Service `.env`:

```env
PORT=3000

DB_HOST=127.0.0.1
DB_PORT=5433
DB_NAME=orders_db
DB_USER=postgres
DB_PASSWORD=postgres
```

The health endpoint currently works:

```http
GET /health
```

Successful response:

```json
{
  "status": "ok",
  "service": "order-service",
  "database": "connected",
  "time": "..."
}
```

Therefore:

```text
Node.js
   ↓
127.0.0.1:5433
   ↓
Docker PostgreSQL :5432
```

is working.

---

# 12. Project Phases

Build the project incrementally.

Do NOT implement everything at once.

---

## PHASE 1 — Infrastructure

Goal:

Build the complete local distributed infrastructure.

Components:

```text
Docker
 │
 ├── PostgreSQL
 ├── Redpanda
 ├── Redis
 └── Debezium
```

Tasks:

- Docker Compose
- PostgreSQL
- PostgreSQL initialization
- WAL configuration
- Redpanda listeners
- Redis
- Debezium/Kafka Connect
- Docker networking
- Health checks

Deliverable:

All containers run successfully.

---

# PHASE 2 — Node.js Order Service

Goal:

Build the basic Order API.

Implement:

```http
GET /health
POST /orders
GET /orders/:id
GET /orders
```

Architecture:

```text
HTTP
 ↓
Routes
 ↓
Controller
 ↓
Service
 ↓
Repository/Database
 ↓
PostgreSQL
```

Use clean separation between:

- Routes
- Controllers
- Services
- Database configuration

At this phase, first understand normal CRUD.

Do not immediately add Kafka or Redis.

---

# PHASE 3 — Transactional Outbox

This is the core phase.

Implement:

```text
POST /orders
      │
      ▼
BEGIN TRANSACTION
      │
      ├── INSERT orders
      │
      └── INSERT outbox_events
      │
      ▼
COMMIT
```

Use the same PostgreSQL connection:

```js
const client = await pool.connect();

try {
    await client.query("BEGIN");

    // insert order

    // insert outbox event

    await client.query("COMMIT");
} catch (error) {
    await client.query("ROLLBACK");
    throw error;
} finally {
    client.release();
}
```

Important concept:

All statements in a PostgreSQL transaction must execute using the same client connection.

Demonstrate failure scenarios:

### Case A

```text
Order INSERT succeeds
Outbox INSERT fails
```

Expected:

```text
Both rollback
```

### Case B

```text
Order INSERT succeeds
Outbox INSERT succeeds
COMMIT
```

Expected:

```text
Both persist
```

This phase should clearly demonstrate how the dual-write problem is solved.

---

# PHASE 4 — PostgreSQL CDC with Debezium

Connect:

```text
PostgreSQL WAL
      ↓
Debezium
      ↓
Redpanda
```

Configure Debezium to capture the outbox table.

Goal:

When:

```sql
INSERT INTO outbox_events
```

happens, an event should appear in Redpanda.

Test:

```text
POST /orders
       ↓
orders row
       +
outbox_events row
       ↓
PostgreSQL WAL
       ↓
Debezium
       ↓
Redpanda topic
```

Verify events using Redpanda CLI/tools.

---

# PHASE 5 — Event-Driven Microservices

Introduce downstream services.

For example:

```text
Order Service
     │
     ▼
Redpanda
     │
     ├── Payment Service
     ├── Inventory Service
     └── Notification Service
```

Each service consumes:

```text
ORDER_CREATED
```

events.

Implement:

### Payment Service

Consumes order events and processes payment.

### Inventory Service

Reserves inventory.

### Notification Service

Sends order confirmation.

These should be independent services.

---

# PHASE 6 — Idempotent Consumers

Important distributed-systems problem:

Kafka/Redpanda generally provides **at-least-once delivery**, so the same event may arrive multiple times.

Example:

```text
ORDER_CREATED
       ↓
Payment Service
       ↓
Payment succeeds
       ↓
Network failure before acknowledgement
       ↓
Broker retries event
       ↓
Payment Service receives same event again
```

Without idempotency:

```text
₹1000 charged
₹1000 charged again
```

This is unacceptable.

Implement:

```text
eventId
idempotencyKey
```

and deduplication.

Architecture:

```text
Kafka Event
     │
     ▼
Idempotency Check
     │
     ├── Already processed → Ignore
     │
     └── New event
            ↓
       Process event
            ↓
       Mark processed
```

Use:

- Redis for fast deduplication
- PostgreSQL for durable idempotency records where appropriate

Explain the tradeoffs.

---

# PHASE 7 — Retry System

Implement failures and retries.

Example:

```text
Payment Service
      ↓
Database timeout
      ↓
Retry
```

Use exponential backoff:

```text
Attempt 1 → immediate
Attempt 2 → 1 sec
Attempt 3 → 2 sec
Attempt 4 → 4 sec
Attempt 5 → 8 sec
```

Add jitter to prevent synchronized retry storms.

Discuss:

- transient failures
- permanent failures
- retryable errors
- non-retryable errors
- retry limits

---

# PHASE 8 — Dead Letter Queue

If an event repeatedly fails:

```text
Kafka
 ↓
Consumer
 ↓
Retry
 ↓
Retry
 ↓
Retry
 ↓
Maximum retries
 ↓
DLQ
```

Example:

```text
orders.events
orders.retry
orders.dlq
```

DLQ should preserve:

- original event
- event ID
- failure reason
- stack/error metadata
- retry count
- timestamp
- service name

Create an operational mechanism to inspect/reprocess DLQ events.

---

# PHASE 9 — Failure Injection & Reliability Testing

Intentionally introduce failures:

- PostgreSQL unavailable
- Redis unavailable
- Redpanda unavailable
- Consumer crash
- Network timeout
- Duplicate events
- Slow database
- Payment failure
- malformed event
- consumer restart

Measure system behavior.

Demonstrate:

```text
No data loss
No double payment
Events eventually processed
Failed events enter DLQ
Services recover automatically
```

This phase makes the project substantially stronger than a normal CRUD portfolio project.

---

# PHASE 10 — Observability

Add:

- OpenTelemetry
- Prometheus
- Grafana
- structured logging
- distributed tracing
- correlation IDs

Track:

### Metrics

```text
orders_created_total
events_published_total
events_processed_total
events_failed_total
consumer_lag
retry_count
dlq_count
processing_latency
database_latency
```

Architecture:

```text
Services
   │
   ├── Logs
   ├── Metrics
   └── Traces
          │
          ▼
   Observability Stack
          │
          ├── Prometheus
          ├── Grafana
          └── Loki / logging system
```

---

# PHASE 11 — AI Incident Response / RAG

Only after the distributed system is working should we add AI.

Do NOT put the LLM directly in the critical order transaction path.

The business path remains:

```text
Client
 ↓
Order Service
 ↓
PostgreSQL
 ↓
Debezium
 ↓
Redpanda
 ↓
Consumers
```

AI belongs in the operational/observability layer.

---

# 13. AI Incident Response Architecture

Build:

```text
Logs
Metrics
Traces
Deployment Events
Service Topology
Historical Incidents
Runbooks
Postmortems
       │
       ▼
Incident Detection
       │
       ▼
Error Fingerprinting
       │
       ▼
Incident Clustering
       │
       ▼
Context Retrieval / RAG
       │
       ▼
LLM / AI Agent
       │
       ▼
Diagnosis
       │
       ▼
Recommended Remediation
       │
       ▼
Policy Engine
       │
       ├── Low Risk → Automatic Action
       │
       └── High Risk → Human Approval
       │
       ▼
Verification
       │
       ▼
Incident Resolved
```

---

# 14. RAG Knowledge Base

Use RAG over:

- runbooks
- previous incidents
- postmortems
- architecture documentation
- service dependency information
- deployment history
- known errors
- operational procedures

Do NOT blindly vectorize millions of raw logs.

Instead:

```text
Raw Logs
   ↓
Normalize
   ↓
Fingerprint
   ↓
Cluster
   ↓
Summarize Incident
   ↓
Retrieve Relevant Knowledge
   ↓
LLM Diagnosis
```

This reduces noise, token usage, and false retrievals.

---

# 15. Example AI Incident

Suppose:

```text
Payment Service
    ↓
PostgreSQL timeout
    ↓
Retries increase
    ↓
Kafka consumer lag increases
    ↓
Order processing latency increases
```

Observability detects:

```text
DB timeout rate ↑
consumer lag ↑
payment failures ↑
```

The AI system retrieves a previous incident:

```text
Previous Incident:
PostgreSQL connection pool exhaustion
Symptoms:
- connection timeout
- payment latency
- consumer lag
Resolution:
- increase pool size
- restart affected consumer
```

The AI produces:

```text
Root Cause:
Likely PostgreSQL connection pool exhaustion.

Evidence:
1. DB timeout rate increased.
2. Payment latency increased simultaneously.
3. Consumer lag increased after DB failures.
4. Similar historical incident exists.

Recommended Action:
Increase available DB connections and restart affected consumer.

Confidence:
0.91
```

Policy engine decides whether this action is safe.

---

# 16. Self-Healing

Eventually implement controlled remediation.

Example low-risk actions:

```text
Restart unhealthy consumer
Pause/resume consumer
Reprocess DLQ event
Scale consumer replicas
Clear temporary cache
```

High-risk actions require approval:

```text
Database schema changes
Production data deletion
Large infrastructure changes
Financial transaction changes
```

Never allow the LLM to directly execute arbitrary infrastructure commands.

Use:

```text
LLM
 ↓
Structured Action
 ↓
Policy Engine
 ↓
Validation
 ↓
Executor
```

---

# 17. Final Architecture

The final platform should look like:

```text
                         ┌──────────────────────┐
                         │      Client          │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   Order Service      │
                         │    Node.js/Express   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │     PostgreSQL       │
                         │                      │
                         │ orders               │
                         │ outbox_events        │
                         └──────────┬───────────┘
                                    │
                                   WAL
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      Debezium        │
                         │        CDC           │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      Redpanda        │
                         │      Kafka API        │
                         └──────┬───┬───┬───────┘
                                │   │   │
                       ┌────────┘   │   └────────┐
                       ▼            ▼            ▼
                  Payment      Inventory    Notification
                   Service       Service       Service
                       │            │            │
                       └──────┬─────┴─────┬──────┘
                              │           │
                              ▼           ▼
                           Redis      PostgreSQL


========================================================

              OBSERVABILITY / AI LAYER

Logs ───────────┐
Metrics ────────┤
Traces ─────────┤
Deployments ────┤
                ▼
       Incident Detection
                │
                ▼
       Error Fingerprinting
                │
                ▼
       Incident Correlation
                │
                ▼
             RAG
                │
                ▼
          AI Diagnosis
                │
                ▼
         Policy Engine
                │
          ┌─────┴─────┐
          ▼           ▼
      Auto Fix    Human Approval
          │           │
          └─────┬─────┘
                ▼
           Verification
                │
                ▼
          Incident Closed
```

---

# 18. What I Want From You as My Mentor

When teaching me, behave like a senior engineer who has designed production distributed systems.

Do NOT simply dump code.

For every major component explain:

1. What problem does it solve?
2. Why do we need it?
3. Why did we choose this technology?
4. How does it work internally?
5. What happens during failure?
6. What are the alternatives?
7. What are the tradeoffs?
8. How does it interact with the other components?
9. How would this work in production?
10. How can I explain it in an interview?

Use concrete examples.

For example, don't just say:

> "Use idempotency."

Explain:

```text
Event arrives
     ↓
eventId = abc123
     ↓
Redis/PostgreSQL lookup
     ↓
Already processed?
     ├── YES → ignore
     └── NO → process
                  ↓
             mark processed
```

---

# 19. Implementation Rules

Follow these rules:

### Rule 1

Build incrementally.

Never introduce Kafka, Redis, Debezium, retries, DLQ, AI, etc. before their phase.

### Rule 2

Use JavaScript/Node.js.

Don't switch to Go or TypeScript unless explicitly requested.

### Rule 3

Use Docker.

Infrastructure should remain reproducible through Docker Compose.

### Rule 4

Explain commands.

When giving:

```powershell
docker compose up -d
```

explain what it does.

### Rule 5

Use Windows PowerShell commands.

My development environment is:

```text
Windows
PowerShell
Docker Desktop
Node.js
```

### Rule 6

Use production-quality patterns.

Avoid toy implementations where possible.

### Rule 7

Always test each phase.

For each phase provide:

```text
Implementation
      ↓
Run
      ↓
Test
      ↓
Expected output
      ↓
Failure test
```

### Rule 8

Do not hide failures.

We should intentionally test failures because reliability is a major objective.

---

# 20. Current Project Status

Completed:

### Phase 1

Docker infrastructure has been created.

Running components:

```text
PostgreSQL 17
Redpanda
Redis 8
Debezium 3.0.0.Final
```

PostgreSQL contains:

```text
orders
outbox_events
```

PostgreSQL is configured for logical WAL.

### Phase 2

Node.js + Express Order Service is running.

Health endpoint:

```http
GET /health
```

works successfully.

Current Node → PostgreSQL connection:

```text
Node.js
   ↓
127.0.0.1:5433
   ↓
Docker PostgreSQL:5432
```

Current successful result:

```json
{
  "status": "ok",
  "service": "order-service",
  "database": "connected"
}
```

---

# 21. Immediate Next Task

Continue from the current state.

Do NOT restart the project from Phase 1.

The next task is:

## Implement `POST /orders`

First implement the normal order creation flow:

```text
POST /orders
     ↓
Controller
     ↓
Order Service
     ↓
PostgreSQL
     ↓
orders
```

Request:

```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "amount": 1499.99
}
```

Then verify:

```sql
SELECT * FROM orders;
```

After that, immediately evolve the implementation into:

```text
BEGIN
   INSERT orders
   INSERT outbox_events
COMMIT
```

and explain exactly why this solves the dual-write problem.

Do not skip the conceptual explanation.

---

# 22. Final Portfolio Goal

The final project should be strong enough to demonstrate:

**Distributed Systems + Backend Engineering + Event-Driven Architecture + Reliability + Observability + AI/RAG**

Potential resume statement:

> Built a fault-tolerant event-driven order processing platform using Node.js, PostgreSQL, Debezium CDC, Redpanda, Redis and Docker, implementing transactional outbox, at-least-once delivery, idempotent consumers, exponential retries and DLQs; extended the platform with an AI-powered incident response engine that correlates logs, metrics, traces and historical operational knowledge to diagnose distributed-system failures and recommend verified remediation.

The project should prioritize **correctness, reliability, explainability, and demonstrable failure handling** over simply adding more technologies.

Start from the **current Phase 2 state** and continue with `POST /orders`.