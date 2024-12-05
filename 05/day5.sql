WITH rules AS (
    SELECT
        column0 AS first,
        column1 AS second,
        row_number() OVER () AS id
    FROM read_csv_auto('input_order.csv', header = false, delim = '|')
),

pages AS (
    SELECT
        string_split_regex(column0, ',')::INTEGER [] AS arr,
        row_number() OVER () AS id
    FROM read_csv_auto('input_pages.csv', header = false, delim = '')
),

test AS (
    SELECT
        *,
        CASE
            WHEN
                NOT EXISTS (
                    SELECT 1
                    FROM rules AS r
                    WHERE
                        list_contains(p.arr, r.first)
                        AND list_contains(p.arr, r.second)
                        AND list_position(p.arr, r.first) > list_position(p.arr, r.second)
                )
                THEN 1 -- Valid
            ELSE 0 -- Invalid
        END AS is_valid
    FROM pages AS p
),

result AS (
    SELECT arr[((len(arr) + 1) / 2)::INTEGER] AS middle_element
    FROM test
    WHERE is_valid
),

invalid_records AS (
    SELECT * FROM test WHERE is_valid = 0
),

flattened_pages AS (
    SELECT
        id,
        arr,
        unnest(arr) AS page,
        len(arr) AS total_pages
    FROM invalid_records
),

page_priorities AS (
    SELECT
        f.id,
        f.page,
        any_value(f.total_pages) AS total_pages,
        sum(
            CASE
                WHEN r.first = f.page THEN 1 -- Higher priority for `first`
                WHEN r.second = f.page THEN -1 -- Lower priority for `second`
                ELSE 0
            END
        ) AS priority

    FROM flattened_pages AS f
    LEFT JOIN rules AS r
        ON (f.page = r.first OR f.page = r.second)
    WHERE
        r.first IN (
            SELECT page FROM flattened_pages WHERE id = f.id
        )
        AND r.second IN (
            SELECT page FROM flattened_pages WHERE id = f.id
        )
    GROUP BY f.id, f.page
    ORDER BY f.id ASC, priority DESC
),

row_order AS (
    SELECT
        *,
        ((total_pages + 1) / 2)::INTEGER AS middle_element,
        row_number() OVER (PARTITION BY id ORDER BY id, priority) AS row_id
    FROM page_priorities
)

SELECT
    sum(middle_element) AS result,
    'part2' AS info
FROM result
UNION ALL
SELECT
    sum(page) AS result,
    'part1' AS info
FROM row_order
WHERE row_id = middle_element
