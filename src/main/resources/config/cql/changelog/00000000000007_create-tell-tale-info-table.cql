CREATE TABLE IF NOT EXISTS tell_tale_info
(
    device_id text,
    iid text,
    ets text,
    sen text,
    state text,
    event_created_date timestamp,
    created_date timestamp,
    primary key (device_id, event_created_date, iid)
);

CREATE TABLE IF NOT EXISTS last_tell_tale_info
(
    device_id text,
    iid text,
    state text,
    event_created_date timestamp,
    primary key (device_id, iid)
);
