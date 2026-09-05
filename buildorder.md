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