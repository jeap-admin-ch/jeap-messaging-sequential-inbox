CREATE TABLE sequential_inbox_idempotence
(
    message_type         text                     not null,
    idempotence_id       text                     not null,
    sequence_instance_id bigint                   not null references sequence_instance ON DELETE CASCADE,
    created_at           timestamp with time zone NOT NULL,
    CONSTRAINT sequential_inbox_idempotence_pkey PRIMARY KEY (message_type, idempotence_id)
);

CREATE INDEX sequential_inbox_idempotence_sequence_instance_id
    ON sequential_inbox_idempotence (sequence_instance_id);
