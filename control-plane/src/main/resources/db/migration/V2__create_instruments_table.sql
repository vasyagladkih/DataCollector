CREATE TABLE instruments (
    instrument_uid VARCHAR(64) PRIMARY KEY,
    ticker VARCHAR(32) NOT NULL,
    class_code VARCHAR(16) NOT NULL,
    instrument_type VARCHAR(16) NOT NULL,
    name VARCHAR(255) NOT NULL,
    lot INT NOT NULL DEFAULT 1,
    currency VARCHAR(10) NOT NULL DEFAULT 'rub',
    is_tradable BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
