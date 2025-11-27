-- Query 2: Find all instances where a Call overlaps with a Tower signal,
-- but exclude cases where the Tower signal is fully contained within the Call duration.
SELECT 
    c.unique_id,
    c.caller,
    t.tower,
    c.start_ts AS call_start,
    c.end_ts AS call_end,
    t.start_ts AS tower_start,
    t.end_ts AS tower_end
FROM mono_signal c
JOIN tower_signal t 
    ON c.unique_id = t.unique_id
WHERE 
    -- 1. Overlap Condition: The two intervals must overlap in some way
    t.start_ts < c.end_ts 
    AND t.end_ts > c.start_ts
    
    -- 2. Exclusion Condition: Exclude cases where Tower is fully inside Call
    AND NOT (
        t.start_ts >= c.start_ts 
        AND t.end_ts <= c.end_ts
    );

-- Query 10: Find gaps between consecutive connected calls for each caller

SELECT
    caller,
    unique_id,
    start_ts,
    end_ts,
    prev_end_ts,
    start_ts - prev_end_ts AS gap_duration
FROM (
    SELECT
        caller,
        unique_id,
        start_ts,
        end_ts,
        LAG(end_ts) OVER (
            PARTITION BY caller
            ORDER BY start_ts
        ) AS prev_end_ts
    FROM mono_signal
    WHERE disposition = 'connected'
) AS x
WHERE prev_end_ts IS NOT NULL AND start_ts > prev_end_ts        -- only consecutive calls
ORDER BY caller, start_ts;