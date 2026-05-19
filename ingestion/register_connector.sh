#!/bin/bash

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="market-pulse-postgres-connector"

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "  Kafka Connect not ready yet, retrying in 3s..."
  sleep 3
done
echo "Kafka Connect is ready."

echo ""
echo "Checking if connector already exists..."
EXISTING=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME" | grep -c "name")

if [ "$EXISTING" -gt 0 ]; then
  echo "Connector already exists. Deleting and re-registering..."
  curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
  sleep 2
fi

echo ""
echo "Registering Debezium Postgres connector..."
curl -s -X POST \
  -H "Content-Type: application/json" \
  --data @ingestion/debezium_postgres_connector.json \
  "$CONNECT_URL/connectors" | python3 -m json.tool

echo ""
echo "Done. Checking connector status in 5s..."
sleep 5

curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool