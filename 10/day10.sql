WITH day10 AS (
    SELECT
        string_split_regex(column0, '') AS arr,
        row_number() OVER () AS col
    FROM read_csv_auto('input.csv', header = false, delim = '')
),

map AS (
    SELECT
        col,
        unnest(arr)::INTEGER as height,
        generate_subscripts(arr, 1) AS row
    FROM day10
),

hiking_trails AS (
    WITH RECURSIVE ctx AS (
        -- Here we can run all recursive runs at the same time
        -- which should be more performant for duckdb engine (and in general :D)
        SELECT
            row,
            col,
            height,
            CAST(row || '-' || col AS VARCHAR) AS trail
        FROM map
        WHERE height = 0

        UNION ALL

        SELECT
            m.row,
            m.col,
            m.height,
            ht.trail || '->' || m.row || '-' || m.col AS trail
        FROM ctx ht
        JOIN map m
            ON (ABS(ht.row - m.row) = 1 AND ht.col = m.col -- vertical
                OR ABS(ht.col - m.col) = 1 AND ht.row = m.row) -- horizontal
            AND m.height = ht.height + 1
    )
    SELECT * FROM ctx
),

valid_trails AS (
    SELECT DISTINCT
        trail[1: strpos(trail,'->')-1] AS trailhead,
        row,
        col
    FROM hiking_trails
    WHERE height = 9
),

part2_valid_trails AS (
    SELECT
        trail[1: strpos(trail,'->')-1] AS trailhead,
        row,
        col
    FROM hiking_trails
    WHERE height = 9
)

SELECT COUNT(*) as result, 'part1' as info
FROM  valid_trails
UNION ALL
SELECT COUNT(*) as result, 'part2' as info
FROM part2_valid_trails

