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
expanded_map_2 AS (
    SELECT
        y,
        x,
        CASE WHEN cell = '#' THEN 1
        ELSE 0
        END AS cell
    FROM expanded_map
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
potential_positions AS (
    WITH RECURSIVE patrol(x, y, direction) AS (
        -- Start at the initial position
        SELECT g.x, g.y, g.direction
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
        FROM patrol p
        JOIN directions d ON p.direction = d.direction
        -- use this join to find if there is an obstacle in the direction
        LEFT JOIN expanded_map_2 e ON p.x + d.dx = e.x AND p.y + d.dy = e.y AND e.cell = 1

        -- Base case of recursion (end of recursion)
        WHERE p.x > 0
            AND p.y > 0
            AND p.x < (SELECT x_bound FROM boundaries)
            AND p.y < (SELECT y_bound FROM boundaries)
    )
    SELECT DISTINCT x, y FROM patrol
),
-- this is unfortunateley too slow
detect_loops2 AS (
    -- Loop through each position in potential_positions and treat it as an obstacle
    SELECT DISTINCT *
    FROM potential_positions pp
    CROSS JOIN LATERAL (
        WITH RECURSIVE patrol(x, y, direction, step, cycle, visited) AS (
            -- Start at the initial position
            SELECT g.x, g.y, g.direction, 1, 1, [g.x || ';' || g.y || g.direction] as visited
            FROM guard_start g

            UNION ALL

            SELECT
                CASE
                    WHEN e.cell IS NULL AND (p.x + d.dx != pp.x OR p.y + d.dy != pp.y) THEN p.x + d.dx
                    ELSE p.x
                END AS x,
                CASE
                    WHEN e.cell IS NULL AND (p.x + d.dx != pp.x OR p.y + d.dy != pp.y) THEN p.y + d.dy
                    ELSE p.y
                END AS y,
                CASE
                    -- turn according to our direction table
                    WHEN e.cell IS NOT NULL OR (p.x + d.dx = pp.x AND p.y + d.dy = pp.y) THEN d.next_direction
                    ELSE p.direction
                END AS direction,
                CASE
                    WHEN (p.x + d.dx || ';' || p.y + d.dy || p.direction) NOT IN visited THEN 0
                    ELSE 1
                END AS cycle,
                p.step + 1,
                list_append(visited, p.x + d.dx || ';' || p.y + d.dy || p.direction) as visited
            FROM patrol p
            JOIN directions d ON p.direction = d.direction
            -- Check if there's an obstacle in the direction
            LEFT JOIN expanded_map e ON p.x + d.dx = e.x AND p.y + d.dy = e.y AND e.cell = '#'
            -- Add the current potential position as an obstacle
            LEFT JOIN (SELECT pp.x AS x, pp.y AS y FROM potential_positions WHERE pp.x = p.x + d.dx AND pp.y = p.y + d.dy) pp1
            ON pp1.x = p.x + d.dx AND pp1.y = p.y + d.dy

            -- Base case of recursion (end of recursion)
            WHERE p.x > 0
                AND p.y > 0
                AND p.x < (SELECT x_bound FROM boundaries)
                AND p.y < (SELECT y_bound FROM boundaries)
                AND p.x + d.dx || ';' || p.y + d.dy || p.direction NOT IN visited
                AND p.step < 5
        )
        SELECT step, cycle, visited
        FROM patrol p
        JOIN directions d ON p.direction = d.direction
        WHERE p.x + d.dx || ';' || p.y + d.dy || p.direction IN visited
    ) pp2
)
SELECT * FROM potential_positions
