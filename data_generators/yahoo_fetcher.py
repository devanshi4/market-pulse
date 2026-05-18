import os
import time
import logging
from datetime import datetime
from decimal import Decimal

import yfinance as yf
import psycopg2
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
TICKERS        = ["AAPL", "GOOGL", "TSLA", "MSFT", "AMZN", "NVDA", "META", "NFLX"]
FETCH_INTERVAL = 60   # seconds between each full fetch cycle


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
    """
    Download the latest 1-day snapshot for every ticker in one API call.
    yfinance returns a dict-like object per ticker with OHLCV fields.
    """
    results = []
    data    = yf.download(
        tickers  = tickers,
        period   = "1d",
        interval = "1m",
        progress = False,   # suppress the yfinance download bar
        threads  = True,
    )

    fetched_at = datetime.utcnow()

    for ticker in tickers:
        try:
            # Get the most recent completed minute bar for this ticker
            ticker_data = data.xs(ticker, axis=1, level=1) if len(tickers) > 1 else data
            latest      = ticker_data.dropna().iloc[-1]

            results.append({
                "ticker":      ticker,
                "open_price":  Decimal(str(round(float(latest["Open"]),   4))),
                "high_price":  Decimal(str(round(float(latest["High"]),   4))),
                "low_price":   Decimal(str(round(float(latest["Low"]),    4))),
                "close_price": Decimal(str(round(float(latest["Close"]),  4))),
                "volume":      int(latest["Volume"]),
                "fetched_at":  fetched_at,
            })

        except Exception as e:
            log.warning("Could not fetch data for %s: %s", ticker, e)

    return results


# ── Write to Postgres ──────────────────────────────────────────────────────────
def insert_prices(conn, prices: list[dict]):
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

    log.info("Inserted %s price rows at %s", len(prices), prices[0]["fetched_at"])
    for p in prices:
        log.info(
            "  %-6s  open=%-10s  high=%-10s  low=%-10s  close=%-10s  vol=%s",
            p["ticker"], p["open_price"], p["high_price"],
            p["low_price"], p["close_price"], p["volume"],
        )


# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    log.info("Connecting to Postgres...")
    conn = get_connection()
    log.info("Connected. Fetching prices every %ss for: %s", FETCH_INTERVAL, TICKERS)

    try:
        while True:
            log.info("Fetching from Yahoo Finance...")
            prices = fetch_prices(TICKERS)

            if prices:
                insert_prices(conn, prices)
            else:
                log.warning("No price data returned — Yahoo Finance may be rate limiting.")

            log.info("Sleeping %ss until next fetch...", FETCH_INTERVAL)
            time.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    main()