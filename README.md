# 🚀 Distributed Transactional Outbox & AI-Assisted Order Processing Platform

> A fault-tolerant, event-driven distributed order processing platform built with Node.js, PostgreSQL, Debezium CDC, Redpanda, Redis, Docker, and an AI-powered incident response layer.

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Problem Statement](#-problem-statement)
- [Solution](#-solution)
- [Key Objectives](#-key-objectives)
- [Architecture](#-architecture)
- [Complete System Flow](#-complete-system-flow)
- [Transactional Outbox Flow](#-transactional-outbox-flow)
- [CDC Flow](#-cdc-flow)
- [Event Processing Flow](#-event-processing-flow)
- [Idempotency Flow](#-idempotency-flow)
- [Retry and DLQ Flow](#-retry-and-dlq-flow)
- [AI Incident Response Architecture](#-ai-incident-response-architecture)
- [Technology Stack](#-technology-stack)
- [Project Structure](#-project-structure)
- [Infrastructure](#-infrastructure)
- [Database Design](#-database-design)
- [Services](#-services)
- [API](#-api)
- [Event Schema](#-event-schema)
- [Reliability Model](#-reliability-model)
- [Failure Scenarios](#-failure-scenarios)
- [Observability](#-observability)
- [AI/RAG Layer](#-airag-layer)
- [Security](#-security)
- [Local Development](#-local-development)
- [Testing](#-testing)
- [Implementation Phases](#-implementation-phases)
- [Performance Goals](#-performance-goals)
- [Future Improvements](#-future-improvements)
- [Interview Talking Points](#-interview-talking-points)
- [License](#-license)

---

# 🧠 Overview

The **Distributed Transactional Outbox & AI-Assisted Order Processing Platform** is a production-oriented distributed backend system designed to solve reliability and consistency problems that occur when multiple microservices communicate through asynchronous events.

The platform demonstrates:

- Transactional Outbox Pattern
- PostgreSQL Write-Ahead Logging (WAL)
- Change Data Capture (CDC)
- Debezium
- Kafka-compatible event streaming using Redpanda
- At-least-once event delivery
- Idempotent consumers
- Redis-based deduplication
- Retry mechanisms
- Exponential backoff
- Dead Letter Queues
- Failure recovery
- Distributed tracing
- Metrics and structured logging
- Incident detection
- RAG-based operational knowledge retrieval
- AI-assisted incident diagnosis
- Controlled self-healing

The core objective is:

> **Guarantee that important business state changes and their corresponding events are persisted reliably, while making downstream event processing resilient to duplicates, failures, retries, and service outages.**

---

# ❗ Problem Statement

In a distributed system, an Order Service often needs to perform two operations:

1. Store the order in a database.
2. Publish an event to a message broker.

A naive implementation might look like:

```text
Client
   |
   v
Order Service
   |
   +------------------+
   |                  |
   v                  v
PostgreSQL          Kafka
   |                  |
   |                  |
Save Order       Publish Event
```

```mermaid
sequenceDiagram
    participant C as Client
    participant O as Order Service
    participant DB as PostgreSQL
    participant K as Redpanda

    C->>O: POST /orders

    O->>DB: INSERT order
    DB-->>O: SUCCESS

    O->>K: Publish ORDER_CREATED
    K-->>O: FAILURE

    O-->>C: What should happen?
```

The order exists, but the event was lost.
The opposite failure is also possible:
Kafka event → SUCCESS
Database transaction → FAILURE

## Solution
This project solves the dual-write problem using the:
Transactional Outbox Pattern
Instead of directly writing to PostgreSQL and Redpanda independently, the Order Service writes:
1. The business entity.
2. The event that represents the state change.
into PostgreSQL within the same database transaction.

```mermaid
flowchart LR

    Client --> OrderService

    OrderService --> Transaction

    subgraph DBTx["PostgreSQL Transaction"]
        Transaction --> Orders[(orders)]
        Transaction --> Outbox[(outbox_events)]
    end

    Outbox --> WAL[PostgreSQL WAL]
    WAL --> Debezium
    Debezium --> Redpanda

    Redpanda --> Payment
    Redpanda --> Inventory
    Redpanda --> Notification
```

## Key Objectives
The system is designed around the following objectives:
Reliability
Prevent loss of important business events.
Consistency
Ensure database state and event state remain consistent.
Fault Tolerance
Recover from temporary failures.
Idempotency
Prevent duplicate processing.
Scalability
Allow consumers to scale horizontally.
Observability
Understand what is happening across services.
AI-Assisted Operations
Use historical operational knowledge and AI to diagnose incidents.

## 🏗 Architecture
### High-Level Architecture

```mermaid
flowchart TB

    Client["Client / API Consumer"]

    Gateway["API Layer"]

    Order["Order Service<br/>Node.js + Express"]

    DB["PostgreSQL 17"]

    WAL["PostgreSQL WAL"]

    CDC["Debezium CDC"]

    Broker["Redpanda<br/>Kafka Compatible"]

    Payment["Payment Service"]

    Inventory["Inventory Service"]

    Notification["Notification Service"]

    Redis["Redis"]

    Client --> Gateway
    Gateway --> Order

    Order --> DB

    DB --> WAL
    WAL --> CDC
    CDC --> Broker

    Broker --> Payment
    Broker --> Inventory
    Broker --> Notification

    Payment --> Redis
    Inventory --> Redis

    Payment --> DB
    Inventory --> DB
    Notification --> DB
```

### 🌐 Complete System Architecture

```mermaid
flowchart TB

    subgraph ClientLayer["Client Layer"]
        Client["Web / Mobile / API Client"]
    end

    subgraph ApplicationLayer["Application Layer"]
        Gateway["API Gateway"]
        OrderService["Order Service"]
        PaymentService["Payment Service"]
        InventoryService["Inventory Service"]
        NotificationService["Notification Service"]
    end

    subgraph DataLayer["Data Layer"]
        PostgreSQL["PostgreSQL"]
        Redis["Redis"]
    end

    subgraph StreamingLayer["Event Streaming"]
        WAL["PostgreSQL WAL"]
        Debezium["Debezium CDC"]
        Redpanda["Redpanda / Kafka"]
    end

    subgraph ReliabilityLayer["Reliability"]
        Retry["Retry Topics"]
        DLQ["Dead Letter Queue"]
        Idempotency["Idempotency / Deduplication"]
    end

    subgraph ObservabilityLayer["Observability"]
        Logs["Structured Logs"]
        Metrics["Prometheus"]
        Traces["OpenTelemetry"]
        Grafana["Grafana"]
    end

    subgraph AILayer["AI Incident Response"]
        Detector["Incident Detector"]
        Fingerprint["Error Fingerprinting"]
        RAG["Operational RAG"]
        Agent["AI Diagnosis Agent"]
        Policy["Policy Engine"]
        Remediation["Remediation Executor"]
    end

    Client --> Gateway
    Gateway --> OrderService

    OrderService --> PostgreSQL

    PostgreSQL --> WAL
    WAL --> Debezium
    Debezium --> Redpanda

    Redpanda --> PaymentService
    Redpanda --> InventoryService
    Redpanda --> NotificationService

    PaymentService --> Idempotency
    InventoryService --> Idempotency

    Idempotency --> Redis
    Idempotency --> PostgreSQL

    PaymentService --> Retry
    InventoryService --> Retry

    Retry --> DLQ

    OrderService --> Logs
    PaymentService --> Logs
    InventoryService --> Logs
    NotificationService --> Logs

    OrderService --> Metrics
    PaymentService --> Metrics
    InventoryService --> Metrics

    OrderService --> Traces
    PaymentService --> Traces
    InventoryService --> Traces

    Logs --> Detector
    Metrics --> Detector
    Traces --> Detector

    Detector --> Fingerprint
    Fingerprint --> RAG
    RAG --> Agent
    Agent --> Policy
    Policy --> Remediation
```

## Complete System Flow
When a client creates an order:

```mermaid
sequenceDiagram

    participant C as Client
    participant O as Order Service
    participant DB as PostgreSQL
    participant W as WAL
    participant D as Debezium
    participant R as Redpanda
    participant P as Payment
    participant I as Inventory
    participant N as Notification

    C->>O: POST /orders

    O->>DB: BEGIN

    O->>DB: INSERT orders

    O->>DB: INSERT outbox_events

    O->>DB: COMMIT

    DB-->>O: Transaction committed

    O-->>C: 201 Created

    DB->>W: Write change to WAL

    W->>D: CDC event

    D->>R: ORDER_CREATED

    R->>P: ORDER_CREATED
    R->>I: ORDER_CREATED
    R->>N: ORDER_CREATED

    P->>P: Process Payment
    I->>I: Reserve Inventory
    N->>N: Send Notification
```

## Transactional Outbox Flow
The critical transaction is:

```mermaid
flowchart TD

    Request["POST /orders"]

    Begin["BEGIN"]

    Order["INSERT INTO orders"]

    Outbox["INSERT INTO outbox_events"]

    Commit["COMMIT"]

    Rollback["ROLLBACK"]

    Request --> Begin
    Begin --> Order
    Order --> Outbox
    Outbox --> Commit

    Order -. Failure .-> Rollback
    Outbox -. Failure .-> Rollback
```

## CDC Flow
PostgreSQL uses a Write-Ahead Log.
The simplified flow is:

```text
PostgreSQL Transaction
        |
        v
PostgreSQL WAL
        |
        v
Debezium
        |
        v
Redpanda
        |
        v
Consumers
```

### WAL
WAL stands for:
Write-Ahead Log
PostgreSQL records database changes in the WAL before final persistence.
Debezium reads these logical changes and converts them into events.

## Event Processing Flow

```mermaid
flowchart LR

    Event["ORDER_CREATED"]

    Broker["Redpanda"]

    Consumer["Consumer"]

    Check["Idempotency Check"]

    Process["Business Processing"]

    Success["Success"]

    Retry["Retry"]

    DLQ["Dead Letter Queue"]

    Broker --> Consumer
    Consumer --> Check

    Check --> Process

    Process --> Success

    Process --> Retry

    Retry --> Consumer

    Retry --> DLQ
```

## Idempotency
The system assumes at-least-once delivery.
Therefore an event can arrive multiple times.

```mermaid
flowchart TD

    Event["Incoming Event"]

    ID["Extract eventId"]

    Check["Check Idempotency Store"]

    Already["Already Processed"]

    New["New Event"]

    Process["Process Business Operation"]

    Mark["Mark Event Processed"]

    Done["ACK Event"]

    Event --> ID
    ID --> Check

    Check --> Already
    Already --> Done

    Check --> New
    New --> Process
    Process --> Mark
    Mark --> Done
```

## Retry System
Not every failure should immediately go to a DLQ.
Transient errors should be retried.
Examples:
- Database timeout
- Network timeout
- Temporary service unavailable
- Temporary broker failure

### Exponential Backoff
Example:
Attempt 1 → 0 sec
Attempt 2 → 1 sec
Attempt 3 → 2 sec
Attempt 4 → 4 sec
Attempt 5 → 8 sec
Jitter should be added in production to avoid synchronized retry storms.

## ☠️ Dead Letter Queue
If an event repeatedly fails:
```text
Main Topic
    |
    v
Consumer
    |
    v
Retry
    |
    v
Retry
    |
    v
Retry
    |
    v
Maximum Attempts
    |
    v
DLQ
```

A DLQ event should retain:
- Event ID
- Original event
- Failure reason
- Retry count
- Service name
- Timestamp
- Error metadata

Example:
```json
{
  "eventId": "abc-123",
  "eventType": "ORDER_CREATED",
  "retryCount": 5,
  "service": "payment-service",
  "error": "Payment provider timeout",
  "failedAt": "2026-09-04T12:00:00Z"
}
```

## AI Incident Response Architecture
The AI layer is intentionally outside the critical order-processing path.
The core business system remains deterministic.

```mermaid
flowchart TB

    Logs["Application Logs"]
    Metrics["Metrics"]
    Traces["Distributed Traces"]
    Deployments["Deployment Events"]
    Topology["Service Topology"]

    Detector["Incident Detection"]

    Fingerprint["Error Fingerprinting"]

    Correlation["Incident Correlation"]

    Knowledge["Historical Incidents<br/>Runbooks<br/>Postmortems"]

    VectorDB["Vector / Search Store"]

    RAG["RAG Retrieval"]

    LLM["AI Diagnosis Agent"]

    Policy["Policy Engine"]

    Approval["Human Approval"]

    Auto["Low-Risk Auto Remediation"]

    Executor["Remediation Executor"]

    Verify["Verification"]

    Closed["Incident Resolved"]

    Logs --> Detector
    Metrics --> Detector
    Traces --> Detector
    Deployments --> Detector
    Topology --> Detector

    Detector --> Fingerprint
    Fingerprint --> Correlation

    Knowledge --> VectorDB
    Correlation --> RAG
    VectorDB --> RAG

    RAG --> LLM
    LLM --> Policy

    Policy --> Auto
    Policy --> Approval

    Auto --> Executor
    Approval --> Executor

    Executor --> Verify
    Verify --> Closed
```

## Technology Stack

| Technology | Purpose |
| ---------- | ------- |
| Node.js | Backend runtime |
| Express.js | REST API |
| PostgreSQL 17 | Primary database |
| PostgreSQL WAL | CDC source |
| Debezium | Change Data Capture |
| Redpanda | Kafka-compatible event streaming |
| Redis 8 | Fast deduplication/idempotency |
| Docker | Local infrastructure |
| Docker Compose | Multi-container orchestration |
| OpenTelemetry | Distributed tracing |
| Prometheus | Metrics |
| Grafana | Dashboards |
| Loki / Logging System | Centralized logs |
| Vector Database | RAG knowledge retrieval |
| LLM | Incident diagnosis |
| Kubernetes | Future production orchestration |

## Project Structure
(To be implemented)

## Infrastructure
(To be implemented)

## Database Design
(To be implemented)

## Services
(To be implemented)

## API
(To be implemented)

## Event Schema
(To be implemented)

## Reliability Model
(To be implemented)

## Failure Scenarios
(To be implemented)

## Observability
(To be implemented)

## AI/RAG Layer
(To be implemented)

## Security
(To be implemented)

## Local Development
(To be implemented)

## Testing
(To be implemented)

## Implementation Phases
(To be implemented)

## Performance Goals
(To be implemented)

## Future Improvements
(To be implemented)

## Interview Talking Points
(To be implemented)

## License
(To be implemented)
