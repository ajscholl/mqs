CREATE TABLE IF NOT EXISTS queues (
	id SERIAL PRIMARY KEY,
	name VARCHAR NOT NULL CONSTRAINT queues_name_unique UNIQUE,
	max_receives INT NULL CONSTRAINT positive_max_receives CHECK (max_receives IS NULL OR max_receives > 0),
	dead_letter_queue VARCHAR NULL REFERENCES queues (name) ON UPDATE CASCADE ON DELETE SET NULL,
	retention_timeout INTERVAL NOT NULL CONSTRAINT positive_retention_timeout CHECK (retention_timeout > INTERVAL '0 second'),
	visibility_timeout INTERVAL NOT NULL CONSTRAINT non_negative_visibility_timeout CHECK (visibility_timeout >= INTERVAL '0 second'),
	message_delay INTERVAL NOT NULL CONSTRAINT message_delay CHECK (message_delay >= INTERVAL '0 second'),
	content_based_deduplication BOOLEAN NOT NULL,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
	id UUID PRIMARY KEY,
	payload BYTEA NOT NULL,
	content_type VARCHAR NOT NULL,
	content_encoding VARCHAR NULL,
	hash VARCHAR NULL,
	queue VARCHAR NOT NULL REFERENCES queues (name) ON UPDATE CASCADE ON DELETE CASCADE,
	receives INT NOT NULL,
	visible_since TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS messages_queue_hash_uidx
	ON messages (queue, hash);

CREATE INDEX IF NOT EXISTS messages_queue_visible_since_idx
	ON messages (queue, visible_since);
CREATE INDEX IF NOT EXISTS messages_queue_visible_since_id_idx
	ON messages (queue, visible_since, id);

SELECT diesel_manage_updated_at('queues');

CREATE INDEX IF NOT EXISTS queues_created_at_idx ON queues (created_at);
CREATE INDEX IF NOT EXISTS queues_updated_at_idx ON queues (updated_at);

CREATE INDEX IF NOT EXISTS messages_created_at_idx ON messages (created_at);
CREATE INDEX IF NOT EXISTS messages_queue_created_at_idx ON messages (queue, created_at);

CREATE OR REPLACE FUNCTION clear_max_receives() RETURNS trigger AS $$
BEGIN
    IF (
        NEW IS DISTINCT FROM OLD AND
        NEW.max_receives IS NOT NULL AND
        NEW.dead_letter_queue IS NULL
    ) THEN
        NEW.max_receives := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER queues_clear_max_receives_on_insert BEFORE INSERT ON queues
    FOR EACH ROW EXECUTE PROCEDURE clear_max_receives();

CREATE TRIGGER queues_clear_max_receives_on_update BEFORE UPDATE ON queues
    FOR EACH ROW EXECUTE PROCEDURE clear_max_receives();
