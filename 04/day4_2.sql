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

-- Diagonal (Top-Left to Bottom-Right)
diagonal_tl_br AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_tl_br' AS direction,
        w2.row_id AS m_row_id,
        w2.col_id AS m_col_id

    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id + 1 AND w2.col_id = w1.col_id + 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id + 2 AND w3.col_id = w1.col_id + 2
    WHERE w1.letter = 'M' AND w2.letter = 'A' AND w3.letter = 'S'
),

-- Diagonal (Bottom-Right to Top-Left)
diagonal_br_tl AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_br_tl' AS direction,
        w2.row_id AS m_row_id,
        w2.col_id AS m_col_id
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id - 1 AND w2.col_id = w1.col_id - 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id - 2 AND w3.col_id = w1.col_id - 2
    WHERE w1.letter = 'M' AND w2.letter = 'A' AND w3.letter = 'S'
),

-- Diagonal (Top-Right to Bottom-Left)
diagonal_bl_tr AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_tr_bl' AS direction,
        w2.row_id AS m_row_id,
        w2.col_id AS m_col_id
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id + 1 AND w2.col_id = w1.col_id - 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id + 2 AND w3.col_id = w1.col_id - 2
    WHERE w1.letter = 'M' AND w2.letter = 'A' AND w3.letter = 'S'
),

-- Diagonal (Bottom-Left to Top-Right)
diagonal_tr_bl AS (
    SELECT
        w1.row_id,
        w1.col_id,
        'diagonal_bl_tr' AS direction,
        w2.row_id AS m_row_id,
        w2.col_id AS m_col_id
    FROM word_search AS w1
    INNER JOIN word_search AS w2 ON w2.row_id = w1.row_id - 1 AND w2.col_id = w1.col_id + 1
    INNER JOIN word_search AS w3 ON w3.row_id = w1.row_id - 2 AND w3.col_id = w1.col_id + 2
    WHERE w1.letter = 'M' AND w2.letter = 'A' AND w3.letter = 'S'
),

all_directions AS (
    -- s . s
    -- . a .
    -- m . m
    SELECT * FROM diagonal_bl_tr AS a
    INNER JOIN diagonal_br_tl AS b ON a.m_row_id = b.m_row_id AND a.m_col_id = b.m_col_id
    UNION ALL

    -- m . s
    -- . a .
    -- m . s
    SELECT * FROM diagonal_bl_tr AS a
    INNER JOIN diagonal_tl_br AS b ON a.m_row_id = b.m_row_id AND a.m_col_id = b.m_col_id
    UNION ALL

    -- m . m
    -- . a .
    -- s . s
    SELECT * FROM diagonal_tl_br AS a
    INNER JOIN diagonal_tr_bl AS b ON a.m_row_id = b.m_row_id AND a.m_col_id = b.m_col_id
    UNION ALL

    -- s . m
    -- . a .
    -- s . m
    SELECT * FROM diagonal_tr_bl AS a
    INNER JOIN diagonal_br_tl AS b ON a.m_row_id = b.m_row_id AND a.m_col_id = b.m_col_id

)

SELECT count(*) FROM all_directions
