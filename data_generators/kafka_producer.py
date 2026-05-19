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
    """
    Create and return a Kafka producer configured for our local broker.
    All scripts that need to publish events import this function.
    """
    config = {
        # Your laptop → Docker, so use localhost and the EXTERNAL port
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        # How many partition replicas must confirm a write before
        # the producer considers it successful.
        # "all" = wait for all in-sync replicas (safest, slight latency cost)
        "acks": "all",
        # Retry up to 3 times if a write fails transiently
        "retries": 3,
        # Wait up to 1 second to batch messages before sending.
        # Improves throughput when many messages are produced quickly.
        "linger.ms": 100,
    }
    return Producer(config)


class DecimalDatetimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles two types Python's default
    encoder cannot serialize:
      - Decimal  → convert to string  (preserves exact precision)
      - datetime → convert to ISO 8601 string  (standard timestamp format)

    Without this, json.dumps() would throw a TypeError on our
    price and timestamp fields.
    """
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
    """
    Serialize payload to JSON and publish it to the given Kafka topic.

    Parameters:
        producer : the Kafka producer instance
        topic    : topic name to publish to
        key      : partition key — events with the same key always
                   go to the same partition (guarantees ordering per key)
        payload  : the event data as a Python dictionary
    """
    try:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, cls=DecimalDatetimeEncoder).encode("utf-8"),
            on_delivery=_delivery_callback,
        )
        # flush() ensures the message is actually sent now rather than
        # sitting in the producer's internal buffer.
        # For low-volume use cases like ours, flush after every message.
        # In high-throughput scenarios you'd flush in batches instead.
        producer.flush()

    except Exception as e:
        log.error("Failed to publish event to topic %s: %s", topic, e)
        raise


def _delivery_callback(err, msg):
    """
    Called automatically by the producer after each message
    is either successfully delivered or permanently failed.
    This is asynchronous — it fires after produce() returns.
    """
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