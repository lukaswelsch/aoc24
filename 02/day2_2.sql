WITH data AS (
    SELECT string_split_regex(column0, ' ') AS arr, row_number() OVER (ORDER BY 1) AS id
    FROM read_csv_auto('input.csv', header = false, delim = '')
),
temp AS (
        WITH expanded AS (
            SELECT
                ID,
                unnest(arr),
                generate_subscripts(arr, 1) AS index,
                generate_series(1, len(arr)) as idx,
                arr
            FROM
                data
        ),
        result AS (
            SELECT
                ID,
                list_select(arr, list_filter(idx, x -> x != index)) as arr,
                'slice' as info,
                row_number() OVER (ORDER BY 1) AS slice_id
            FROM
                expanded
        )
        SELECT * FROM result
),
check_validity AS (
    SELECT
        ID,
        array_zip(arr, arr[2:-1])[1:-2] AS paired_values,
        slice_id
    FROM temp
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
        slice_id,
    FROM paired_values

),
group_Validation AS (
    SELECT slice_id, ID
    FROM validation
    GROUP BY ID, slice_id
    HAVING MIN(is_valid_row) > 0
),
ordered_rows AS (
    SELECT
        ID,
        UNNEST(arr) AS number,
        generate_subscripts(arr, 1) AS index,
        slice_id

    FROM temp
),
prepared_Data AS (
    SELECT *,

            CASE
                WHEN number::INTEGER > LAG(number::INTEGER) OVER (PARTITION BY ID ORDER BY slice_id, index) THEN 'increasing'
                WHEN number::INTEGER < LAG(number::INTEGER) OVER (PARTITION BY ID ORDER BY slice_id, index) THEN 'decreasing'
                ELSE 'same'
            END AS trend
    FROM ordered_rows
),
ordered_data AS (
    SELECT ID, slice_id
    FROM prepared_Data
    WHERE index != 1
    GROUP BY ID, slice_id
    HAVING COUNT(DISTINCT trend) = 1

),
grouped AS (SELECT o.ID
            FROM validation v
                JOIN ordered_data o ON v.ID = o.ID
                AND o.slice_id = v.slice_id
                JOIN group_validation gv
                ON gv.slice_id = v.slice_id
                AND v.id = o.id
            WHERE o.ID IN (SELECT ID FROM group_validation)

                GROUP BY o.ID
)


-- dont forget to also include the rows that are not modified
-- SELECT *
-- FROM grouped
-- LEFT JOIN t1
-- ON grouped.ID = t1.ID
-- WHERE t1.ID is null
--
SELECT * FROM grouped