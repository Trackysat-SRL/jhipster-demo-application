CREATE TABLE IF NOT EXISTS daily_aggregation
(
    device_id    text,
    "timestamp" timestamp,
    positions text,
    primary key (device_id)
);

CREATE TABLE IF NOT EXISTS monthly_aggregation
(
    device_id    text,
    "timestamp" timestamp,
    positions text,
    primary key (device_id)
);
