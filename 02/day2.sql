CREATE OR REPLACE TABLE t1 AS
WITH data AS (
    SELECT
        string_split_regex(column0, ' ') AS arr,
        row_number() OVER (ORDER BY 1) AS id
    FROM read_csv_auto('input.csv', header = false, delim = '')
),

check_validity AS (
    SELECT
        id,
        array_zip(arr, arr[2:-1])[1:-2] AS paired_values
    FROM data
),

paired_values AS (
    SELECT
        *,
        unnest(paired_values) AS pair
    FROM check_validity
),

validation AS (
    SELECT
        CASE
            WHEN abs(pair[1]::INTEGER - pair[2]::INTEGER) IN (1, 2, 3) -- Differences must be 1 or 2 or 3
                THEN 1
            ELSE 0
        END AS is_valid_row,
        id
    FROM paired_values
),

ordered_rows AS (
    SELECT
        id,
        unnest(arr) AS number,
        generate_subscripts(arr, 1) AS index

    FROM data
),

ordered_data AS (
    SELECT id FROM (
        SELECT
            *,

            CASE
                WHEN number::INTEGER > lag(number::INTEGER) OVER (PARTITION BY id ORDER BY index) THEN 'increasing'
                WHEN number::INTEGER < lag(number::INTEGER) OVER (PARTITION BY id ORDER BY index) THEN 'decreasing'
                ELSE 'same'
            END AS trend
        FROM ordered_rows

    )
    WHERE index != 1
    GROUP BY id
    HAVING count(DISTINCT trend) = 1

),

grouped AS (
    SELECT o.id
    FROM validation AS v
    INNER JOIN ordered_data AS o ON v.id = o.id
    GROUP BY o.id
    HAVING min(is_valid_row) > 0
)
-- SELECT COUNT(*) as result, 'part1' AS info
-- FROM grouped

SELECT * FROM grouped
