docker exec -it  order-postgres psql -U postgres -d orders_db


curl.exe http://localhost:8083/   for (You should get a response indicating Kafka Connect/Debezium is running.)

Also check connectors: curl.exe http://localhost:8083/connectors

Register it with Debezium::
curl.exe -X POST "http://localhost:8083/connectors" `
  -H "Content-Type: application/json" `
  --data-binary "@infrastructure/debezium/postgres-connector.json"

## Check connector status
  curl.exe "http://localhost:8083/connectors/postgres-orders-connector/status"

## List Redpanda topics
 docker compose exec redpanda rpk topic list

## Consume the topic
 docker compose exec redpanda rpk topic consume ORDER.events

 docker compose exec redpanda rpk topic consume ORDER.events -n 1


 curl.exe -X POST "http://localhost:3000/orders" -H "Content-Type: application/json" -d '{\"userId\":\"550e8400-e29b-41d4-a716-446655440000\",\"amount\":1499.99}'

## "Debezium captures the outbox insert from PostgreSQL, and the Outbox Event Router transforms that CDC record into an application event on the ORDER.events topic. The routed event uses the aggregate ID as the Kafka key, the outbox ID as the event ID header, and the outbox payload as the event value."