WITH data AS (
    SELECT
        regexp_extract_all(column0, 'mul\([0-9]+,[0-9]+\)') AS arr,
        'part1' AS info
    FROM read_csv_auto('input.csv', header = false, delim = '')
    UNION ALL
    SELECT
        regexp_extract_all('do()' || string_agg(column0, '') || 'don''t()', 'do\(\).*?don\''t\(\)') AS arr,
        'part2' AS info
    FROM read_csv_auto('input.csv', header = false, delim = '')

),

extracted_data AS (
    SELECT
        info,
        unnest(arr) AS calculation
    FROM data
),

extracted_2 AS (
    SELECT
        info,
        unnest(regexp_extract_all(calculation, 'mul\([0-9]+,[0-9]+\)')) AS calculation
    FROM extracted_data
),

further AS (
    SELECT
        info,
        regexp_extract(calculation, 'mul'),
        str_split_regex(regexp_extract(calculation, '[0-9]+,[0-9]+'), ',') AS nbrs
    FROM extracted_2
),

multiplied_data AS (
    SELECT
        info,
        nbrs[1]::INTEGER * nbrs[2]::INTEGER AS multiplied
    FROM further
)

SELECT
    info,
    sum(multiplied) AS result
FROM multiplied_data
GROUP BY info
ORDER BY info
