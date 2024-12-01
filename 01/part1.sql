-- duckdb can't use a delimiter of more than 1 byte, so this will use a regex to load the data
WITH data AS (
    SELECT string_split_regex(column0, '   ') as arr
        FROM read_csv_auto('input.csv', header=false, delim='')
),
extracted AS (
    SELECT arr[1]::INTEGER as first, arr[2]::INTEGER as second
    FROM data
),
-- the puzzle needs us to independently sort the columns
-- for this we have to create a sort column for each column
extracted_first AS (
    SELECT first, ROW_NUMBER() OVER (ORDER BY first ASC) as rn
    FROM extracted
),
extracted_second AS (
    SELECT second, ROW_NUMBER() OVER (ORDER BY second ASC) as rn
    FROM extracted
)
SELECT SUM(abs(second-first)) AS part1
FROM extracted_first ef
JOIN extracted_second es
ON ef.rn = es.rn
