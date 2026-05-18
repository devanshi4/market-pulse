import os
import time
import random
import logging
from datetime import datetime
from decimal import Decimal

import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
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

SLEEP_SECONDS   = 3    # insert a trade every 3 seconds
PRICE_VARIATION = 0.03 # price moves up to ±3% from base


# ── Database connection ────────────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ── Price simulation ───────────────────────────────────────────────────────────
def simulate_price(base_price: float) -> Decimal:
    """
    Apply a small random walk to the base price.
    This makes the data feel like real market movement.
    """
    variation = random.uniform(-PRICE_VARIATION, PRICE_VARIATION)
    price = base_price * (1 + variation)
    return Decimal(str(round(price, 4)))


# ── Core trade logic ───────────────────────────────────────────────────────────
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
    """
    Update the positions table to reflect the new trade.
    If a position doesn't exist yet for this account + ticker, create it.
    If it does exist, update shares_held and recalculate avg_buy_price.

    UPSERT = INSERT ... ON CONFLICT DO UPDATE
    This is a single atomic SQL statement — no need to check first then insert.
    """
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
    """
    Pick a random account, ticker, side, and quantity.
    Insert the trade and update the position inside one transaction.
    """
    ticker     = random.choice(list(TICKERS.keys()))
    base_price = TICKERS[ticker]
    price      = simulate_price(base_price)
    trade_type = random.choice(["BUY", "SELL"])
    quantity   = random.randint(1, 50)
    account_id = random.randint(1, 10)   # we seeded 10 accounts in 01_schema.sql

    with conn.cursor() as cur:
        trade_id = insert_trade(cur, account_id, ticker, trade_type, quantity, price)
        upsert_position(cur, account_id, ticker, trade_type, quantity, price)
        conn.commit()

    log.info(
        "trade_id=%-5s  account=%-3s  %s  %-5s  qty=%-3s  price=$%s",
        trade_id, account_id, trade_type, ticker, quantity, price,
    )


# ── Main loop ──────────────────────────────────────────────────────────────────
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