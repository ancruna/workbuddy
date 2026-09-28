CREATE TABLE IF NOT EXISTS buddy_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    application_id TEXT NOT NULL,
    template_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    template_version INTEGER NOT NULL,
    snapshot_format_version INTEGER NOT NULL,
    config_json TEXT NOT NULL,
    config_digest TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN buddy_snapshot_id TEXT;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN buddy_binding_json TEXT;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_sessions_buddy_snapshot_id
    ON sessions(buddy_snapshot_id);
