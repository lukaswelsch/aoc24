WITH data AS (
    SELECT
        string_split_regex(column0, ' ') AS arr,
        row_number() OVER () AS id
    FROM read_csv_auto('input.csv', header = false, delim = '')
),

expanded AS (
    SELECT
        id,
        arr,
        unnest(arr),
        generate_subscripts(arr, 1) AS index,
        generate_series(1, len(arr)) AS idx
    FROM
        data
),

temp AS (
    SELECT
        id,
        'part1' AS info,
        arr,
        row_number() OVER () * -1 AS slice_id
    FROM expanded
    UNION ALL
    SELECT
        id,
        'part2' AS info,
        list_select(arr, list_filter(idx, x -> x != index)) AS arr,
        row_number() OVER () AS slice_id
    FROM
        expanded
),

paired_values AS (
    SELECT
        *,
        unnest(array_zip(arr, arr[2:-1])[1:-2]) AS pair
    FROM temp
),

validation AS (
    SELECT
        *,
        CASE
            WHEN abs(pair[1]::INTEGER - pair[2]::INTEGER) IN (1, 2, 3) -- Differences must be 1 or 2 or 3
                THEN 1
            ELSE 0
        END AS is_valid_row
    FROM paired_values
),

group_validation AS (
    SELECT
        slice_id,
        id,
        info
    FROM validation
    GROUP BY id, slice_id, info
    HAVING min(is_valid_row) > 0
),

ordered_rows AS (
    SELECT
        *,
        unnest(arr) AS number,
        generate_subscripts(arr, 1) AS index
    FROM temp
),

prepared_data AS (
    SELECT
        *,
        CASE
            WHEN number::INTEGER > lag(number::INTEGER) OVER (PARTITION BY id ORDER BY slice_id, index) THEN 'increasing'
            WHEN number::INTEGER < lag(number::INTEGER) OVER (PARTITION BY id ORDER BY slice_id, index) THEN 'decreasing'
            ELSE 'same'
        END AS trend
    FROM ordered_rows
),

ordered_data AS (
    SELECT
        id,
        slice_id,
        info
    FROM prepared_data
    WHERE index != 1
    GROUP BY id, slice_id, info
    HAVING count(DISTINCT trend) = 1
),

grouped AS (
    SELECT
        o.id,
        o.info
    FROM group_validation AS gv
    INNER JOIN ordered_data AS o
        ON
            gv.id = o.id
            AND gv.slice_id = o.slice_id
    GROUP BY o.id, o.info
)

SELECT
    info,
    count(*) AS result
FROM grouped
GROUP BY info
ORDER BY info
