-- Magic-link (passwordless) login — adds single-use login-link columns to
-- subscribers. Replaces emailing a plaintext generated password at signup.
--
-- Idempotent: safe to run multiple times.
-- Apply manually against the shared DB (no Alembic, no auto-apply).

ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS magic_link_hash VARCHAR(64) NULL;
ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS magic_link_expires_at TIMESTAMPTZ NULL;
ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS magic_link_used_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_subscribers_magic_link_hash
    ON subscribers (magic_link_hash)
    WHERE magic_link_hash IS NOT NULL;
