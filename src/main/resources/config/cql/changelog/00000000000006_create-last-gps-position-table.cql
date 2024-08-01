CREATE TABLE IF NOT EXISTS last_gps_position
(
    device_id           text,
    event_position_date timestamp,
    gps_position        text,
    lat double,
    lon double,
    primary key (device_id)
);
