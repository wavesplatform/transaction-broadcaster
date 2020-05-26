CREATE TABLE IF NOT EXISTS sequences (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    broadcasted_count INTEGER NOT NULL DEFAULT 0,
    total_count INTEGER NOT NULL,
    state SMALLINT NOT NULL DEFAULT 0,
    error_message VARCHAR,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT sequences_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS sequences_txs (
    sequence_id BIGINT NOT NULL REFERENCES sequences (id) ON DELETE CASCADE,
    tx_id VARCHAR NOT NULL,
    state SMALLINT NOT NULL DEFAULT 0,
    error_message VARCHAR,
    position_in_sequence INTEGER,
    tx VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT sequences_txs_pk PRIMARY KEY (sequence_id, tx_id)
);
 