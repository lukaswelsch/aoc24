WITH day11 AS (
    SELECT
        string_split_regex(column0, ' ') AS arr
    FROM read_csv_auto('input.csv', header = false, delim = '')
),

unnested AS (
    SELECT
        unnest(arr)::INT128 as value,
        1::INT128 AS count
    FROM day11
),

arrangement AS (
    WITH RECURSIVE  blink_step(iteration, value, count) AS (
        -- Base case: the initial unique stones
        -- in the input all are initially unique
        SELECT 0 AS iteration, value, count
        FROM unnested

        UNION ALL

        SELECT
            iteration + 1,
            new_value,
            SUM(new_count) AS total_count
        FROM (
            SELECT
                iteration,
                CASE
                    WHEN value = 0 THEN 1
                    WHEN len(value::TEXT) % 2 = 0 THEN
                        left(value::TEXT, (len(value::TEXT) / 2)::BIGINT)::BIGINT
                    ELSE value * 2024
                END AS new_value,
                count AS new_count
            FROM blink_step

            UNION ALL

            SELECT
                iteration,
                right(value::TEXT, (len(value::TEXT) / 2)::BIGINT)::BIGINT AS new_value,
                count AS new_count
            FROM blink_step
            WHERE len(value::TEXT) % 2 = 0
        ) transformations
        WHERE iteration <= 75
        GROUP BY iteration, new_value
    )
    SELECT * FROM blink_step
)

SELECT SUM(count) AS result, 'part1' as info
FROM arrangement
WHERE iteration = 25
GROUP BY iteration
UNION ALL
SELECT SUM(count) AS  result, 'part2' as info
FROM arrangement
WHERE iteration = 75
GROUP BY iteration
