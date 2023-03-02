CREATE TABLE IF NOT EXISTS trackysat_event
(
    device_id    text,
    created_date timestamp,
    event_date   timestamp,
    uid          text,
    ver          text,
    des          text,
    ets          text,
    ori          text,
    con          text,
    PRIMARY KEY ((device_id, created_date))
);

CREATE TABLE IF NOT EXISTS dead_letter_queue
(
    event_id     text,
    created_date timestamp,
    data         text,
    exception    text,
    processed    boolean,
    PRIMARY KEY ((event_id, created_date))
);
