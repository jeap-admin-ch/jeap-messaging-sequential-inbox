ALTER TABLE sequence_instance
    ADD COLUMN remove_after timestamp with time zone;

CREATE INDEX idx_sequence_instance_remove_after ON sequence_instance(remove_after);
