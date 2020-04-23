DROP TRIGGER IF EXISTS queues_clear_max_receives_on_insert ON queues;
DROP TRIGGER IF EXISTS queues_clear_max_receives_on_update ON queues;

DROP FUNCTION IF EXISTS clear_max_receives();

DROP TRIGGER IF EXISTS set_updated_at ON queues;

DROP INDEX IF EXISTS messages_queue_visible_since_idx;
DROP INDEX IF EXISTS messages_queue_visible_since_id_idx;
DROP INDEX IF EXISTS messages_created_at_idx;
DROP INDEX IF EXISTS messages_queue_created_at_idx;

DROP INDEX IF EXISTS queues_created_at_idx;
DROP INDEX IF EXISTS queues_updated_at_idx;

DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS queues;
