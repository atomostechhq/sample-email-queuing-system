-- init.sql
CREATE TABLE IF NOT EXISTS email_logs (
    id SERIAL PRIMARY KEY,
    recipient VARCHAR(255) NOT NULL,
    subject TEXT,
    message_id TEXT,
    status VARCHAR(50) NOT NULL, -- e.g. 'sent' or 'failed'
    error TEXT,
    attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
