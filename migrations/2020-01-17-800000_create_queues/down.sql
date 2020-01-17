DROP TRIGGER IF EXISTS set_updated_at ON queues;

DROP INDEX IF EXISTS messages_queue_visible_since_idx;
DROP INDEX IF EXISTS messages_queue_visible_since_id_idx;
DROP INDEX IF EXISTS messages_created_at_idx;
DROP INDEX IF EXISTS messages_queue_created_at_idx;

DROP INDEX IF EXISTS queues_created_at_idx;
DROP INDEX IF EXISTS queues_updated_at_idx;

DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS queues;
