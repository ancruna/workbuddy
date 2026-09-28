ALTER TABLE sessions ADD COLUMN transport TEXT NOT NULL DEFAULT 'local';
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN conversation_origin TEXT;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN visibility TEXT;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN group_id TEXT;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN group_title TEXT;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN agent_dirty INTEGER;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN agent_dirty_at INTEGER;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN agent_last_synced INTEGER;
--> statement-breakpoint
ALTER TABLE sessions ADD COLUMN verified_at INTEGER;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_sessions_cloud_sort
    ON sessions(user_id, last_activity_at, created_at)
    WHERE transport = 'cloud' AND deleted_at = -1;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_sessions_cloud_group
    ON sessions(user_id, conversation_origin, group_id)
    WHERE transport = 'cloud' AND deleted_at = -1;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_sessions_cloud_project
    ON sessions(user_id, project_id, last_activity_at)
    WHERE transport = 'cloud' AND deleted_at = -1;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_sessions_cloud_verified
    ON sessions(user_id, verified_at)
    WHERE transport = 'cloud';
--> statement-breakpoint
DROP TABLE IF EXISTS cloud_conversations;
