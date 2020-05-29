CREATE TABLE IF NOT EXISTS sequences (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    state SMALLINT NOT NULL,
    error_message VARCHAR DEFAULT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT sequences_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS sequences_txs (
    sequence_id BIGINT NOT NULL REFERENCES sequences (id) ON DELETE CASCADE,
    tx_id VARCHAR NOT NULL,
    state SMALLINT NOT NULL,
    height INT DEFAULT NULL,
    error_message VARCHAR DEFAULT NULL,
    position_in_sequence INTEGER NOT NULL,
    tx VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT sequences_txs_pk PRIMARY KEY (sequence_id, tx_id)
);
 