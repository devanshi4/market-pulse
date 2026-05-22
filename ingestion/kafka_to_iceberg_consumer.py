import os
import json
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from pyiceberg.catalog.sql import SqlCatalog
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BATCH_FLUSH_SECONDS = 30
BATCH_SIZE          = 100

TOPIC_TABLE_MAP = {
    "market_pulse.public.trades":    "bronze.trades",
    "market_pulse.public.positions": "bronze.positions",
    "market_pulse.public.accounts":  "bronze.accounts",
    "market_prices":                 "bronze.market_prices",
}


def get_catalog() -> SqlCatalog:
    bucket   = os.getenv("GCS_BUCKET")
    project  = os.getenv("GCP_PROJECT_ID")
    creds    = os.path.abspath(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    cat_db   = os.getenv("ICEBERG_CATALOG_DB", "market_pulse_catalog.db")
    cat_name = os.getenv("ICEBERG_CATALOG_NAME", "market_pulse")

    return SqlCatalog(
        cat_name,
        **{
            "uri":                         f"sqlite:///{cat_db}",
            "warehouse":                   f"gs://{bucket}/bronze",
            "gcs.project-id":              project,
            "gcs.oauth2.credentials-file": creds,
        },
    )


def get_kafka_consumer(topics: list[str]) -> Consumer:
    config = {
        "bootstrap.servers":  os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "group.id":           "iceberg-bronze-writer-v3",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(config)
    consumer.subscribe(topics)
    log.info("Subscribed to topics: %s", topics)
    return consumer


def sanitize_value(v):
    """
    Convert any Python value into a type PyArrow handles
    natively without needing an explicit schema.

    Decimal → str       preserves exact precision e.g. "182.4500"
    bytes   → str       decodes binary data to text
    datetime → str      converts to ISO string since some Iceberg
                        fields store timestamps as StringType
    bool    → str       Debezium sends __deleted as boolean True/False
                        we convert to "true"/"false" string
    Everything else     int, float, str, None — PyArrow handles natively
    """
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bytes):
        return v.decode("utf-8")
    if isinstance(v, bool):
        # Must check bool BEFORE int because in Python,
        # bool is a subclass of int — isinstance(True, int) is True
        # so if we checked int first, True would become 1
        return str(v).lower()
    if isinstance(v, datetime):
        # Keep datetime objects as-is for _ingested_at
        # which is TimestamptzType in Iceberg.
        # For string fields like fetched_at, the field filtering
        # in write_batch_to_iceberg handles the type correctly
        # because PyArrow sees a real datetime object.
        return v
    return v


def sanitize_row(row: dict) -> dict:
    return {k: sanitize_value(v) for k, v in row.items()}


def parse_message(msg) -> dict | None:
    try:
        raw_value = msg.value()
        if raw_value is None:
            return None

        payload = json.loads(raw_value.decode("utf-8"))

        # Add pipeline metadata
        payload["_ingested_at"]     = datetime.now(timezone.utc)
        payload["_kafka_topic"]     = msg.topic()
        payload["_kafka_partition"] = msg.partition()
        payload["_kafka_offset"]    = msg.offset()

        headers  = dict(msg.headers() or [])
        op_bytes = headers.get("__op")
        payload["_cdc_op"] = op_bytes.decode("utf-8") if op_bytes else ""

        return sanitize_row(payload)

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        log.warning(
            "Could not parse message topic=%s partition=%s offset=%s: %s",
            msg.topic(), msg.partition(), msg.offset(), e
        )
        return None


def write_batch_to_iceberg(
    catalog: SqlCatalog,
    table_name: str,
    batch: list[dict],
) -> None:
    if not batch:
        return

    iceberg_table = catalog.load_table(table_name)

    # Get only the columns this table expects.
    # This drops:
    # - __deleted  (Debezium adds this for DELETE events)
    # - _cdc_op    (not in market_prices schema)
    # - Any other unexpected fields
    expected_cols = {field.name for field in iceberg_table.schema().fields}

    filtered_batch = [
        {k: v for k, v in row.items() if k in expected_cols}
        for row in batch
    ]

    # For string fields that received a datetime object
    # (e.g. fetched_at in market_prices), convert to ISO string now
    # so PyArrow infers string type rather than timestamp
    string_fields = {
        field.name
        for field in iceberg_table.schema().fields
        if str(field.field_type) == "StringType()"
    }

    final_batch = []
    for row in filtered_batch:
        cleaned = {}
        for k, v in row.items():
            if k in string_fields and isinstance(v, datetime):
                cleaned[k] = v.isoformat()
            else:
                cleaned[k] = v
        final_batch.append(cleaned)

    # Let PyArrow infer schema from sanitized values
    arrow_table = pa.Table.from_pylist(final_batch)
    iceberg_table.append(arrow_table)

    log.info("Wrote %s rows to %s", len(batch), table_name)


def main():
    log.info("Connecting to Iceberg catalog...")
    catalog = get_catalog()

    log.info("Connecting to Kafka...")
    topics   = list(TOPIC_TABLE_MAP.keys())
    consumer = get_kafka_consumer(topics)

    batch_buffer: dict[str, list[dict]] = {
        table: [] for table in TOPIC_TABLE_MAP.values()
    }

    last_flush_time = time.time()
    log.info("Starting consumer loop. Waiting for messages...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass

            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug(
                        "End of partition: topic=%s partition=%s",
                        msg.topic(), msg.partition()
                    )
                else:
                    log.error("Kafka error: %s", msg.error())

            else:
                parsed = parse_message(msg)
                if parsed is not None:
                    table_name = TOPIC_TABLE_MAP[msg.topic()]
                    batch_buffer[table_name].append(parsed)

            now           = time.time()
            time_to_flush = (now - last_flush_time) >= BATCH_FLUSH_SECONDS
            flushed_any   = False

            for table_name, buffer in batch_buffer.items():
                if len(buffer) == 0:
                    continue
                if len(buffer) >= BATCH_SIZE or time_to_flush:
                    log.info(
                        "Flushing %s messages to %s (time_flush=%s)",
                        len(buffer), table_name, time_to_flush
                    )
                    write_batch_to_iceberg(catalog, table_name, buffer)
                    batch_buffer[table_name] = []
                    flushed_any = True

            if flushed_any or time_to_flush:
                consumer.commit(asynchronous=False)
                last_flush_time = now
                log.info("Offsets committed to Kafka.")

    except KeyboardInterrupt:
        log.info("Stopped by user.")

    finally:
        log.info("Flushing remaining buffers before shutdown...")
        for table_name, buffer in batch_buffer.items():
            if buffer:
                write_batch_to_iceberg(catalog, table_name, buffer)
        consumer.commit(asynchronous=False)
        consumer.close()
        log.info("Consumer closed cleanly.")


if __name__ == "__main__":
    main()