WITH day7 AS (
    SELECT
        column0 as goal,
        string_split_regex(column1, ' ')[2:] AS arr,
        row_number() OVER () AS y
    FROM read_csv_auto('input.csv', header = false, delim = ':')
),
expanded AS (
    SELECT
        result,
        unnest(arr)::INTEGER as number,
        generate_subscripts(arr, 1) AS row_id,
        len(arr) as num_count
    FROM day7
),
operator AS (
    SELECT 'mul' as op
    UNION ALL
    SELECT 'add' as op
),
recurs AS (
    WITH RECURSIVE calc(goal, current_val, arr, operation ) AS (
            -- Base case: start with goal, current_val = 0 for addition and current_val = 1 for multiplication
            SELECT goal, arr[1]::BIGINT AS current_val, arr[2:] AS arr, 'add' AS operation
            FROM day7

            UNION ALL

            SELECT
                c.goal,
                CASE WHEN o.op = 'add' THEN
                    current_val + arr[1]::BIGINT
                ELSE current_val * arr[1]::BIGINT
                END AS current_val,
                arr[2:] AS arr,
                o.op as operation
            FROM calc c
            CROSS JOIN operator o
              WHERE array_length(arr) > 0
              AND goal != current_val
              AND current_val < goal

    )
    SELECT DISTINCT goal, current_val FROM calc
),
part1 AS (
    SELECT SUM(goal) FROM recurs
    WHERE goal = current_val
),
operator2 AS (
    SELECT 'mul' as op
    UNION ALL
    SELECT 'add' as op
    UNION ALL
    SELECT 'concat' as op
),
recurs2 AS (
    WITH RECURSIVE calc(goal, current_val, arr, operation ) AS (
            SELECT goal, arr[1]::BIGINT AS current_val, arr[2:] AS arr, 'add' AS operation
            FROM day7

            UNION ALL

            SELECT
                c.goal,
                CASE WHEN o.op = 'add' THEN
                        current_val + arr[1]::BIGINT
                    WHEN o.op = 'concat' THEN
                        (current_val || arr[1]::BIGINT)::BIGINT
                    ELSE current_val * arr[1]::BIGINT
                END AS current_val,
                arr[2:] AS arr,
                o.op as operation
            FROM calc c
            CROSS JOIN operator2 o
              WHERE array_length(arr) > 0
              AND goal != current_val
              AND current_val < goal

    )
    SELECT DISTINCT goal, current_val FROM calc
    WHERE goal = current_val
)
SELECT SUM(goal) as result, 'part1' as info FROM recurs
UNION ALL
SELECT SUM(goal) AS result, 'part2' as info FROm recurs2
