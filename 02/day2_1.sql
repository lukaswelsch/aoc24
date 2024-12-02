WITH data AS (
    SELECT string_split_regex(column0, ' ') AS arr, row_number() OVER (ORDER BY 1) AS id
    FROM read_csv_auto('input.csv', header = false, delim = '')
),
check_validity AS (
    SELECT
        ID,
        array_zip(arr, arr[2:-1])[1:-2] AS paired_values
    FROM data
),
paired_values AS (
    SELECT
        *,
        UNNEST(paired_values) AS pair,
    FROM check_validity
),
validation AS (
    SELECT
            CASE
                WHEN ABS(pair[1]::INTEGER - pair[2]::INTEGER) IN (1, 2, 3) -- Differences must be 1 or 2 or 3
                THEN 1
                ELSE 0
            END AS is_valid_row,
        ID,
    FROM paired_values
),
ordered_rows AS (
    SELECT
        ID,
        UNNEST(arr) AS number,
        generate_subscripts(arr, 1) AS index

    FROM data
),
ordered_data AS (
    SELECT ID FROM (
                SELECT *,

                CASE
                    WHEN number::INTEGER > LAG(number::INTEGER) OVER (PARTITION BY ID ORDER BY index) THEN 'increasing'
                    WHEN number::INTEGER < LAG(number::INTEGER) OVER (PARTITION BY ID ORDER BY index) THEN 'decreasing'
                    ELSE 'same'
                END AS trend
        FROM ordered_rows

    )
    --WHERE index != 1
    GROUP BY ID
    HAVING COUNT(DISTINCT trend) = 1

),
grouped AS (SELECT o.ID
            FROM validation v
                JOIN ordered_data o ON v.ID = o.ID
            GROUP BY o.ID
            HAVING MIN(is_valid_row) > 0
)
-- SELECT COUNT(*) as result, 'part1' AS info
-- FROM grouped
--


SELECT * FROM grouped