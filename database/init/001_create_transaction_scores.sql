CREATE TABLE IF NOT EXISTS transaction_scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT transaction_scores_score_range CHECK (score >= 0 AND score <= 1),
    CONSTRAINT transaction_scores_fraud_flag_binary CHECK (fraud_flag IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_created_at
    ON transaction_scores (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_fraud_created_at
    ON transaction_scores (created_at DESC)
    WHERE fraud_flag = 1;
