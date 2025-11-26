CREATE TABLE bi_signal (
    unique_id   INT,
    event_type  INT,
    caller      VARCHAR(20),
    callee      VARCHAR(20),
    timestamp   TIMESTAMP(3),
    disposition VARCHAR(50),
    imei        BIGINT,
    row_id      INT
);
CREATE TABLE mono_signal (
    unique_id   INT,
    start_ts    TIMESTAMP(3),
    end_ts      TIMESTAMP(3),
    caller      VARCHAR(20),
    callee      VARCHAR(20),
    disposition VARCHAR(50),
    imei        BIGINT,
    row_id      INT
);
CREATE TABLE tower_signal (
    unique_id INT,
    tower     VARCHAR(10),
    start_ts  TIMESTAMP(3),
    end_ts    TIMESTAMP(3),
    row_id    INT
);
CREATE TABLE bi_tower_signal (
    unique_id   INT,
    tower       VARCHAR(10),
    event_type  INT,
    timestamp   TIMESTAMP(3),
    row_id      INT
);

