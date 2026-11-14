CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE market_trades (
   exchange TEXT NOT NULL,
   symbol TEXT NOT NULL,
   ticker TEXT NOT NULL,
   price NUMERIC NOT NULL,
   quantity BIGINT NOT NULL,
   trade_time TIMESTAMP WITH TIME ZONE NOT NULL
);

SELECT create_hypertable('market_trades', 'trade_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

ALTER TABLE market_trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol',
    timescaledb.compress_orderby = 'trade_time DESC'
);

SELECT add_compression_policy('market_trades', INTERVAL '1 day');
