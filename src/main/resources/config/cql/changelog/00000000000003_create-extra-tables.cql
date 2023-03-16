CREATE TABLE IF NOT EXISTS tracky_event
(
    device_id    text,
    created_date timestamp,
    con          text,
    des          text,
    ets          text,
    event_date   timestamp,
    ori          text,
    uid          text,
    ver          text,
    primary key (device_id, created_date)
);

CREATE TABLE IF NOT EXISTS device
(
    uid    text,
    created_date timestamp,
    cla          text,
    ver          text,
    typ          text,
    primary key (uid)
);

CREATE TABLE IF NOT EXISTS job_status
(
    job_id text,
    updated_date timestamp,
    lock boolean,
    data text,
    primary key (job_id)
);
