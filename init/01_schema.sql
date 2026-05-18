-- Accounts table: represents brokerage customers
CREATE TABLE IF NOT EXISTS accounts (
    account_id   SERIAL PRIMARY KEY,
    owner_name   VARCHAR(100)        NOT NULL,
    email        VARCHAR(150) UNIQUE NOT NULL,
    balance      NUMERIC(15, 2)      NOT NULL DEFAULT 10000.00,
    created_at   TIMESTAMP           NOT NULL DEFAULT NOW()
);

-- Trades table: every buy/sell order placed by any account
CREATE TABLE IF NOT EXISTS trades (
    trade_id     SERIAL PRIMARY KEY,
    account_id   INT                 NOT NULL REFERENCES accounts(account_id),
    ticker       VARCHAR(10)         NOT NULL,
    trade_type   VARCHAR(4)          NOT NULL CHECK (trade_type IN ('BUY', 'SELL')),
    quantity     INT                 NOT NULL CHECK (quantity > 0),
    price        NUMERIC(10, 4)      NOT NULL,
    traded_at    TIMESTAMP           NOT NULL DEFAULT NOW()
);

-- Positions table: current holdings per account per ticker
CREATE TABLE IF NOT EXISTS positions (
    position_id  SERIAL PRIMARY KEY,
    account_id   INT                 NOT NULL REFERENCES accounts(account_id),
    ticker       VARCHAR(10)         NOT NULL,
    shares_held  NUMERIC(10, 4)      NOT NULL DEFAULT 0,
    avg_buy_price NUMERIC(10, 4),
    last_updated TIMESTAMP           NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, ticker)
);

INSERT INTO accounts (owner_name, email, balance) VALUES
    ('Alice Morgan',   'alice@example.com',   50000.00),
    ('Bob Chen',       'bob@example.com',     75000.00),
    ('Carol Davis',    'carol@example.com',   30000.00),
    ('David Kim',      'david@example.com',  100000.00),
    ('Eva Patel',      'eva@example.com',     45000.00),
    ('Frank Liu',      'frank@example.com',   60000.00),
    ('Grace Okafor',   'grace@example.com',   25000.00),
    ('Henry Russo',    'henry@example.com',   80000.00),
    ('Iris Tanaka',    'iris@example.com',    35000.00),
    ('James Wilson',   'james@example.com',   90000.00);