import os
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal

import yfinance as yf
import psycopg2
from dotenv import load_dotenv

from data_generators.kafka_producer import get_kafka_producer, publish_event

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
TICKERS        = ["AAPL", "GOOGL", "TSLA", "MSFT", "AMZN", "NVDA", "META", "NFLX"]
FETCH_INTERVAL = 60
KAFKA_TOPIC    = "market_prices"


# ── Database connection ────────────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ── Fetch from Yahoo Finance ───────────────────────────────────────────────────
def fetch_prices(tickers: list[str]) -> list[dict]:
    results    = []
    data       = yf.download(
        tickers  = tickers,
        period   = "1d",
        interval = "1m",
        progress = False,
        threads  = True,
    )
    fetched_at = datetime.now(timezone.utc)

    for ticker in tickers:
        try:
            ticker_data = data.xs(ticker, axis=1, level=1) if len(tickers) > 1 else data
            latest      = ticker_data.dropna().iloc[-1]

            results.append({
                "ticker":      ticker,
                "open_price":  Decimal(str(round(float(latest["Open"]),  4))),
                "high_price":  Decimal(str(round(float(latest["High"]),  4))),
                "low_price":   Decimal(str(round(float(latest["Low"]),   4))),
                "close_price": Decimal(str(round(float(latest["Close"]), 4))),
                "volume":      int(latest["Volume"]),
                "fetched_at":  fetched_at,
            })

        except Exception as e:
            log.warning("Could not fetch data for %s: %s", ticker, e)

    return results


# ── Write to Postgres ──────────────────────────────────────────────────────────
def insert_prices_postgres(conn, prices: list[dict]):
    with conn.cursor() as cur:
        for p in prices:
            cur.execute(
                """
                INSERT INTO market_prices
                    (ticker, open_price, high_price, low_price,
                     close_price, volume, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    p["ticker"], p["open_price"], p["high_price"],
                    p["low_price"], p["close_price"], p["volume"],
                    p["fetched_at"],
                ),
            )
        conn.commit()
    log.info("Inserted %s price rows into Postgres", len(prices))


# ── Publish to Kafka ───────────────────────────────────────────────────────────
def publish_prices_kafka(producer, prices: list[dict]):
    """
    Publish each price snapshot as a separate Kafka event.
    Key = ticker symbol so all AAPL events go to the same partition.
    """
    for p in prices:
        publish_event(
            producer=producer,
            topic=KAFKA_TOPIC,
            key=p["ticker"],
            payload=p,
        )
    log.info("Published %s price events to Kafka topic '%s'", len(prices), KAFKA_TOPIC)


# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    log.info("Connecting to Postgres...")
    conn     = get_connection()

    log.info("Connecting to Kafka...")
    producer = get_kafka_producer()

    log.info(
        "Connected to both. Fetching prices every %ss for: %s",
        FETCH_INTERVAL, TICKERS
    )

    try:
        while True:
            log.info("Fetching from Yahoo Finance...")
            prices = fetch_prices(TICKERS)

            if prices:
                # Write to both destinations — Postgres for persistence,
                # Kafka for real-time streaming consumers
                insert_prices_postgres(conn, prices)
                publish_prices_kafka(producer, prices)

                log.info("Cycle complete — next fetch in %ss", FETCH_INTERVAL)
            else:
                log.warning("No price data returned from Yahoo Finance.")

            time.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        conn.close()
        log.info("Postgres connection closed.")


if __name__ == "__main__":
    main()