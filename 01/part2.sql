WITH data AS (
    SELECT string_split_regex(column0, '   ') as arr
        FROM read_csv_auto('input.csv', header=false, delim='')
),
extracted AS (
    SELECT arr[1]::INTEGER as first, arr[2]::INTEGER as second
    FROM data
)
SELECT SUM(ef.first) AS part2
FROM extracted ef
JOIN extracted es
ON ef.first = es.second
