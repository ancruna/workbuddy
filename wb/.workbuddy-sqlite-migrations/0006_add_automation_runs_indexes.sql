CREATE INDEX IF NOT EXISTS idx_automation_runs_automation_id
    ON automation_runs(automation_id);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS idx_automation_runs_created_at
    ON automation_runs(created_at);
