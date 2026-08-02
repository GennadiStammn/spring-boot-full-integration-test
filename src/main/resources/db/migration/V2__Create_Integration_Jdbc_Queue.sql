CREATE SEQUENCE int_message_seq START WITH 1 INCREMENT BY 1 NO CYCLE;

CREATE TABLE int_channel_message (
    message_id CHAR(36) NOT NULL,
    group_key CHAR(36) NOT NULL,
    created_date BIGINT NOT NULL,
    message_priority BIGINT,
    message_sequence BIGINT NOT NULL DEFAULT nextval('int_message_seq'),
    message_content BYTEA,
    region VARCHAR(100) NOT NULL,
    CONSTRAINT int_channel_message_pk PRIMARY KEY (region, group_key, created_date, message_sequence)
);

CREATE INDEX int_channel_msg_delete_idx ON int_channel_message (region, group_key, message_id);
