"""Shared Kafka producer and JSON publishing helper."""
import os
import json
import logging
from datetime import datetime
from decimal import Decimal
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)


def get_kafka_producer() -> Producer:
    config = {
        # Scripts run on the host, so they use the broker's external listener.
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        # Wait for all in-sync replicas: a little slower, but no silently lost prices.
        "acks": "all",
        "retries": 3,
        # Batch for up to 100 ms before sending.
        "linger.ms": 100,
    }
    return Producer(config)


class DecimalDatetimeEncoder(json.JSONEncoder):
    """Serialize Decimal as a string (no float rounding on prices) and datetime as ISO 8601."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def publish_event(
    producer: Producer,
    topic: str,
    key: str,
    payload: dict,
) -> None:
    """Publish one JSON event. The key picks the partition, so events for the same
    key (a ticker) stay in order."""
    try:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, cls=DecimalDatetimeEncoder).encode("utf-8"),
            on_delivery=_delivery_callback,
        )
        # Volume is low (a few events a minute), so flushing per message is fine.
        # At higher volume this would flush per batch instead.
        producer.flush()

    except Exception as e:
        log.error("Failed to publish event to topic %s: %s", topic, e)
        raise


def _delivery_callback(err, msg):
    """Log the delivery result for each message."""
    if err:
        log.error(
            "Message delivery failed | topic=%s | error=%s",
            msg.topic(), err
        )
    else:
        log.debug(
            "Message delivered | topic=%s | partition=%s | offset=%s",
            msg.topic(), msg.partition(), msg.offset()
        )