-- duckdb can't use a delimiter of more than 1 byte, so this will use a regex to load the data
WITH data AS (
    SELECT string_split_regex(column0, '   ') AS arr
    FROM read_csv_auto('input.csv', header = false, delim = '')
),

extracted AS (
    SELECT
        arr[1]::INTEGER AS location_1,
        arr[2]::INTEGER AS location_2
    FROM data
),

-- the puzzle needs us to independently sort the columns
-- for this we have to create a sort column for each column
extracted_sorted AS (
    SELECT
        *,
        row_number() OVER (ORDER BY location_1 ASC) AS rn_first,
        row_number() OVER (ORDER BY location_2 ASC) AS rn_second
    FROM extracted
)

-- part1:
SELECT
    sum(abs(es.location_2 - ef.location_1)) AS result,
    'part1' AS info
FROM extracted_sorted AS ef
INNER JOIN extracted_sorted AS es
    ON ef.rn_first = es.rn_second

UNION ALL

-- part2:
SELECT
    sum(ef.location_1) AS result,
    'part2' AS info
FROM extracted AS ef
INNER JOIN extracted AS es
    ON ef.location_1 = es.location_2
