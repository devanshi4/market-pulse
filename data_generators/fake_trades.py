"""Simulated brokerage: place a random BUY or SELL every few seconds and keep positions in sync.

Trades land in Postgres, and Debezium picks them up from the write-ahead log.
"""
import os
import time
import random
import logging
from datetime import datetime
from decimal import Decimal

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TICKERS = {
    "AAPL":  182.00,
    "GOOGL": 175.00,
    "TSLA":  175.00,
    "MSFT":  415.00,
    "AMZN":  185.00,
    "NVDA":  875.00,
    "META":  500.00,
    "NFLX":  625.00,
}

SLEEP_SECONDS   = 3     # one trade every 3 seconds
PRICE_VARIATION = 0.03  # up to ±3% from the base price


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def simulate_price(base_price: float) -> Decimal:
    """Return the base price moved by a random amount within ±PRICE_VARIATION."""
    variation = random.uniform(-PRICE_VARIATION, PRICE_VARIATION)
    price = base_price * (1 + variation)
    return Decimal(str(round(price, 4)))


def insert_trade(cursor, account_id: int, ticker: str,
                 trade_type: str, quantity: int, price: Decimal):
    cursor.execute(
        """
        INSERT INTO trades (account_id, ticker, trade_type, quantity, price, traded_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING trade_id
        """,
        (account_id, ticker, trade_type, quantity, price, datetime.utcnow()),
    )
    return cursor.fetchone()[0]


def upsert_position(cursor, account_id: int, ticker: str,
                    trade_type: str, quantity: int, price: Decimal):
    """Apply a trade to the position. A BUY upserts and recalculates the weighted
    average buy price in one statement; a SELL reduces shares and never goes below zero."""
    if trade_type == "BUY":
        cursor.execute(
            """
            INSERT INTO positions (account_id, ticker, shares_held, avg_buy_price, last_updated)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (account_id, ticker)
            DO UPDATE SET
                avg_buy_price = (
                    (positions.shares_held * positions.avg_buy_price)
                    + (EXCLUDED.shares_held * EXCLUDED.avg_buy_price)
                ) / NULLIF(positions.shares_held + EXCLUDED.shares_held, 0),
                shares_held  = positions.shares_held + EXCLUDED.shares_held,
                last_updated = EXCLUDED.last_updated
            """,
            (account_id, ticker, quantity, price, datetime.utcnow()),
        )
    else:  # SELL
        cursor.execute(
            """
            UPDATE positions
            SET    shares_held  = GREATEST(shares_held - %s, 0),
                   last_updated = %s
            WHERE  account_id = %s AND ticker = %s
            """,
            (quantity, datetime.utcnow(), account_id, ticker),
        )


def generate_trade(conn):
    """Generate one random trade. The trade and its position update commit together,
    so the CDC stream never sees one without the other."""
    ticker     = random.choice(list(TICKERS.keys()))
    base_price = TICKERS[ticker]
    price      = simulate_price(base_price)
    trade_type = random.choice(["BUY", "SELL"])
    quantity   = random.randint(1, 50)
    account_id = random.randint(1, 10)   # init/01_schema.sql seeds 10 accounts

    with conn.cursor() as cur:
        trade_id = insert_trade(cur, account_id, ticker, trade_type, quantity, price)
        upsert_position(cur, account_id, ticker, trade_type, quantity, price)
        conn.commit()

    log.info(
        "trade_id=%-5s  account=%-3s  %s  %-5s  qty=%-3s  price=$%s",
        trade_id, account_id, trade_type, ticker, quantity, price,
    )


def main():
    log.info("Connecting to Postgres...")
    conn = get_connection()
    log.info("Connected. Starting trade generator — inserting every %ss", SLEEP_SECONDS)

    try:
        while True:
            generate_trade(conn)
            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    main()