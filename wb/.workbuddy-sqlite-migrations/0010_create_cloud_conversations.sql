CREATE TABLE IF NOT EXISTS cloud_conversations (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    title TEXT,
    conversation_origin TEXT,
    visibility TEXT,
    is_user_defined_title INTEGER,
    status TEXT NOT NULL,
    session_status TEXT,
    lifecycle TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_activity_at INTEGER,
    deleted_at INTEGER,
    project_id TEXT,
    work_dir TEXT,
    is_playground INTEGER,
    pin_order INTEGER NOT NULL DEFAULT 0,
    runtime_id TEXT,
    task_id TEXT,
    task_title TEXT,
    agent_id TEXT,
    sandbox_id TEXT,
    agent_dirty INTEGER,
    agent_dirty_at INTEGER,
    agent_last_synced INTEGER,
    cached_at INTEGER NOT NULL,
    last_fetched_at INTEGER NOT NULL,
    verified_at INTEGER,
    cache_source TEXT NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_cloudconv_user_sort
    ON cloud_conversations(user_id, last_activity_at, created_at);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_cloudconv_user_workdir
    ON cloud_conversations(user_id, work_dir, last_activity_at);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_cloudconv_user_playground
    ON cloud_conversations(user_id, is_playground, last_activity_at);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_cloudconv_user_project
    ON cloud_conversations(user_id, project_id, last_activity_at);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_cloudconv_user_pin
    ON cloud_conversations(user_id, pin_order);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_cloudconv_user_lifecycle
    ON cloud_conversations(user_id, lifecycle, deleted_at);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_cloudconv_verified_at
    ON cloud_conversations(user_id, verified_at);
