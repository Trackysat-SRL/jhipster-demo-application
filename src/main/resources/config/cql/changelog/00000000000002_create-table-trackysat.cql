CREATE TABLE IF NOT EXISTS tracky_position (
    customer_id text,
    created_date timestamp,
    "trigger" text,
    PRIMARY KEY ((customer_id, created_date))
);
