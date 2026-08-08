ALTER TABLE mango_swarm_tasks ADD COLUMN IF NOT EXISTS series_id uuid NULL;

CREATE INDEX IF NOT EXISTS idx_mango_tasks_series
    ON mango_swarm_tasks (series_id, id)
    WHERE series_id IS NOT NULL;
