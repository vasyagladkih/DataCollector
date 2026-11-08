CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS market_trades (
    exchange VARCHAR(16) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    ticker VARCHAR(32) NOT NULL,
    price NUMERIC(18, 4) NOT NULL,
    quantity BIGINT NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL
);

SELECT create_hypertable('market_trades', 'trade_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_market_trades_symbol_time ON market_trades (exchange, symbol, trade_time DESC);

ALTER TABLE market_trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'trade_time DESC'
);

SELECT add_compression_policy('market_trades', INTERVAL '1 day');
