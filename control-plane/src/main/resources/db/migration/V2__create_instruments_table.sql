CREATE TABLE instruments (
     instrument_uid TEXT PRIMARY KEY,
     ticker TEXT NOT NULL,
     class_code TEXT NOT NULL,
     instrument_type TEXT NOT NULL,
     name TEXT NOT NULL,
     lot INT NOT NULL DEFAULT 1,
     currency TEXT NOT NULL,
     is_tradable BOOLEAN NOT NULL DEFAULT false,
     updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
