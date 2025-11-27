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


