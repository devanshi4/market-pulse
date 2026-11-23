#!/bin/bash
# Register the Debezium Postgres connector with Kafka Connect.
# Database credentials come from .env (the same values docker-compose uses), so the
# connector JSON in git only holds ${POSTGRES_*} placeholders.
set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="market-pulse-postgres-connector"
TEMPLATE="ingestion/debezium_postgres_connector.json"

# Fill the ${POSTGRES_*} placeholders from the environment or .env (read literally, the way
# docker compose does, so passwords with $ or quotes survive). Never written to disk.
render_config() {
  python3 - "$TEMPLATE" <<'PY'
import json, os, re, sys
env = {}
if os.path.exists(".env"):
    for line in open(".env"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")
env.update({k: v for k, v in os.environ.items() if k.startswith("POSTGRES_")})

def fill(m):
    if m.group(1) not in env:
        sys.exit(f"{m.group(1)} is not set (add it to .env)")
    return json.dumps(env[m.group(1)])[1:-1]  # escape for a JSON string

print(re.sub(r"\$\{(POSTGRES_[A-Z_]+)\}", fill, open(sys.argv[1]).read()))
PY
}

# Fail early if a credential is missing.
render_config > /dev/null

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "  Kafka Connect not ready yet, retrying in 3s..."
  sleep 3
done
echo "Kafka Connect is ready."

echo ""
echo "Checking if connector already exists..."
if curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null; then
  echo "Connector already exists. Deleting and re-registering..."
  curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
  sleep 2
fi

echo ""
echo "Registering Debezium Postgres connector..."
render_config | curl -s -X POST \
  -H "Content-Type: application/json" \
  --data @- \
  "$CONNECT_URL/connectors" | python3 -c 'import json,sys; d=json.load(sys.stdin); d.get("config",{}).pop("database.password",None); print(json.dumps(d,indent=2))'

echo ""
echo "Done. Checking connector status in 5s..."
sleep 5

curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool
