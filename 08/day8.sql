WITH day8 AS (
    SELECT
        string_split_regex(column0, '') AS arr,
        row_number() OVER () AS x
    FROM read_csv_auto('test.csv', header = false, delim = '')
),
expanded AS (
    SELECT
        x,
        unnest(arr) as cell,
        generate_subscripts(arr, 1) AS y,
        len(arr) as bound --symmetrical again
    FROM day8

),
antennas AS (
    SELECT *
    FROM expanded
    WHERE cell != '.'
),
antenna_pairs AS (
    SELECT a1.y AS y1, a1.x AS x1, a2.y AS y2, a2.x AS x2, a1.cell, a1.bound
    FROM antennas a1
    JOIN antennas a2 ON a1.cell = a2.cell
    WHERE (a1.y, a1.x) < (a2.y, a2.x)
),
antinodes AS (

    SELECT
        (y1 + (y1 - y2)) AS ay1, (x1 + (x1 - x2)) AS ax1,
        (y2 + (y2 - y1)) AS ay2, (x2 + (x2 - x1)) AS ax2,
        bound
    FROM antenna_pairs
),
valid_antinodes AS (
    SELECT *
    FROM antinodes
    WHERE ay1 BETWEEN 0 AND bound +1
            AND ax1 BETWEEN 0 and bound+1
            AND ay2 BETWEEN 0 and bound+1
            AND ax2 BETWEEN 0 and bound+1
),
all_antinodes AS (
    SELECT ay1 AS y, ax1 AS x FROM valid_antinodes
    UNION ALL
    SELECT ay2 AS y, ax2 AS x FROM valid_antinodes
)
SELECT *
FROM all_antinodes
