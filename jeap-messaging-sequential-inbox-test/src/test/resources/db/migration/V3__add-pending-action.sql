ALTER TABLE sequenced_message
    ADD COLUMN pending_action text;

ALTER TABLE sequence_instance
    ADD COLUMN pending_action text;
