WITH map AS (
    SELECT
        string_split_regex(column0, '') AS arr,
        row_number() OVER () AS y
    FROM read_csv_auto('input.csv', header = false, delim = '')
),
expanded_map AS (
    SELECT
        y,
        unnest(arr) as cell,
        generate_subscripts(arr, 1) AS x
    FROM map
),
boundaries AS (
    SELECT len(ANY_VALUE(arr)) as x_bound, MAX(y) as y_bound
    FROM map
),
guard_start AS (
    -- find initial position, this is always ^
    SELECT x, y, 'U' as direction
    FROM expanded_map
    WHERE cell = '^'
),
directions AS (
    SELECT * FROM (
        VALUES
            ('U', 0, -1, 'R'), -- Up -> Right
            ('R', 1, 0, 'D'),  -- Right -> Down
            ('D', 0, 1, 'L'),  -- Down -> Left
            ('L', -1, 0, 'U')  -- Left -> Up
    ) AS t(direction, dx, dy, next_direction)
),
path AS (
    WITH RECURSIVE patrol(x, y, direction, step) AS (
        -- Start at the initial position
        SELECT g.x, g.y, g.direction, 1
        FROM guard_start g

        UNION ALL

        SELECT
            CASE
                WHEN e.cell IS NULL THEN p.x + d.dx
                ELSE p.x
            END AS x,
            CASE
                WHEN e.cell IS NULL THEN p.y + d.dy
                ELSE p.y
            END AS y,
            CASE
                -- turn according to our direction table
                -- if not an obstacle (e.cell is empty) keep the direction
                WHEN e.cell IS NOT NULL THEN d.next_direction
                ELSE p.direction
            END AS direction,
            p.step + 1
        FROM patrol p
        JOIN directions d ON p.direction = d.direction
        -- use this join to find if there is an obstacle in the direction
        LEFT JOIN expanded_map e ON p.x + d.dx = e.x AND p.y + d.dy = e.y AND e.cell = '#'

        -- Base case of recursion (end of recursion)
        WHERE p.x > 0
            AND p.y > 0
            AND p.x < (SELECT x_bound FROM boundaries)
            AND p.y < (SELECT y_bound FROM boundaries)
    )
    SELECT DISTINCT x, y FROM patrol
),
result AS (
    SELECT COUNT(*) AS distinct_positions FROM path
)
SELECT * FROM result;