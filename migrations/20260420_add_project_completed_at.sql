ALTER TABLE crop_projects ADD COLUMN completed_at DATETIME;

UPDATE crop_projects
SET status = 'planned'
WHERE status IS NULL;
