WITH data AS (
    SELECT
        string_split_regex(column0, '') AS arr,
        row_number() OVER () AS id
    FROM read_csv_auto('input.csv', header = false, delim = '')
),

word_search AS (

    SELECT
        id AS col_id,
        unnest(arr) AS letter,
        generate_subscripts(arr, 1) AS row_id
    FROM data
),

-- Horizontal (left-to-right)
horizontal_lr AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'horizontal_lr' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w1.row_id = w2.row_id AND w2.col_id = w1.col_id + 1
    INNER JOIN word_search AS w3 ON w1.row_id = w3.row_id AND w3.col_id = w1.col_id + 2
    INNER JOIN word_search AS w4 ON w1.row_id = w4.row_id AND w4.col_id = w1.col_id + 3
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

-- Horizontal (right-to-left)
horizontal_rl AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'horizontal_rl' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w1.row_id = w2.row_id AND w2.col_id = w1.col_id - 1
    INNER JOIN word_search AS w3 ON w1.row_id = w3.row_id AND w3.col_id = w1.col_id - 2
    INNER JOIN word_search AS w4 ON w1.row_id = w4.row_id AND w4.col_id = w1.col_id - 3
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

-- Vertical (Top-to-Bottom)
vertical_tb AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'vertical_tb' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id + 1 AND w1.col_id = w2.col_id
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id + 2 AND w1.col_id = w3.col_id
    INNER JOIN word_search AS w4 ON w4.row_id = w1.row_id + 3 AND w1.col_id = w4.col_id
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

-- Vertical (Bottom-to-Top)
vertical_bt AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'vertical_bt' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id - 1 AND w1.col_id = w2.col_id
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id - 2 AND w1.col_id = w3.col_id
    INNER JOIN word_search AS w4 ON w4.row_id = w1.row_id - 3 AND w1.col_id = w4.col_id
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

-- Diagonal (Top-Left to Bottom-Right)
diagonal_tl_br AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_tl_br' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id + 1 AND w2.col_id = w1.col_id + 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id + 2 AND w3.col_id = w1.col_id + 2
    INNER JOIN word_search AS w4 ON w4.row_id = w1.row_id + 3 AND w4.col_id = w1.col_id + 3
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

-- Diagonal (Bottom-Right to Top-Left)
diagonal_br_tl AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_br_tl' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id - 1 AND w2.col_id = w1.col_id - 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id - 2 AND w3.col_id = w1.col_id - 2
    INNER JOIN word_search AS w4 ON w4.row_id = w1.row_id - 3 AND w4.col_id = w1.col_id - 3
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

-- Diagonal (Top-Right to Bottom-Left)
diagonal_tr_bl AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_tr_bl' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id + 1 AND w2.col_id = w1.col_id - 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id + 2 AND w3.col_id = w1.col_id - 2
    INNER JOIN word_search AS w4 ON w4.row_id = w1.row_id + 3 AND w4.col_id = w1.col_id - 3
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

-- Diagonal (Bottom-Left to Top-Right)
diagonal_bl_tr AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_bl_tr' AS direction
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id - 1 AND w2.col_id = w1.col_id + 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id - 2 AND w3.col_id = w1.col_id + 2
    INNER JOIN word_search AS w4 ON w4.row_id = w1.row_id - 3 AND w4.col_id = w1.col_id + 3
    WHERE w1.letter = 'X' AND w2.letter = 'M' AND w3.letter = 'A' AND w4.letter = 'S'
),

all_directions AS (
    SELECT * FROM horizontal_lr
    UNION ALL
    SELECT * FROM horizontal_rl
    UNION ALL
    SELECT * FROM vertical_tb
    UNION ALL
    SELECT * FROM vertical_bt
    UNION ALL
    SELECT * FROM diagonal_tl_br
    UNION ALL
    SELECT * FROM diagonal_br_tl
    UNION ALL
    SELECT * FROM diagonal_tr_bl
    UNION ALL
    SELECT * FROM diagonal_bl_tr
)

-- SELECT COUNT(*), direction AS total_occurrences FROM all_directions GROUP BY direction;
SELECT
    'part1' AS info,
    count(*) AS result
FROM all_directions
-- SELECT * FROM word_search
