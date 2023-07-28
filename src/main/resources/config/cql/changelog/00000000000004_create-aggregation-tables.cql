CREATE TABLE IF NOT EXISTS daily_aggregation
(
    device_id       text,
    aggregated_date timestamp,
    positions       text,
    primary key (device_id, aggregated_date)
);

CREATE TABLE IF NOT EXISTS monthly_aggregation
(
    device_id       text,
    aggregated_date timestamp,
    positions       text,
    primary key (device_id, aggregated_date)
);

alter table daily_aggregation
    add sensors text;

alter table monthly_aggregation
    add sensors text;


CREATE TABLE IF NOT EXISTS daily_aggregation_error
(
    device_id       text,
    aggregated_date timestamp,
    positions       text,
    error			text,
    primary key (device_id, aggregated_date)
);
