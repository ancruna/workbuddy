ALTER TABLE automation_runs ADD COLUMN failure_code TEXT;
--> statement-breakpoint
ALTER TABLE automation_runs ADD COLUMN reason_code TEXT;