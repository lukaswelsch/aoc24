WITH day9 AS (
    SELECT
        string_split_regex(column0, '') AS disk_map
    FROM read_csv_auto('input.csv', all_varchar=true, header = false, delim = '')
),

parsed_map AS (
    SELECT
        unnest(disk_map)::BIGINT as value,
        generate_subscripts(disk_map, 1) -1 AS idx
    FROM day9
),

expanded_map AS (
    SELECT
        CASE WHEN MOD(idx, 2) = 0 THEN
            [(idx/2)::BIGINT::VARCHAR for x in range(0, value::BIGINT)]
        ELSE
            ['.' for x in range(0, value::BIGINT)]
        END AS expanded_block,
        CASE WHEN MOD(idx, 2) != 0 THEN
            value
        END AS free_space,
        value AS total_space,
        1 as join_id
    FROM parsed_map
),

flattened_map AS (
    SELECT SUM(free_space) as free_space, SUM(total_space) as len_map,  join_id
    FROM expanded_map
        GROUP BY join_id
),

extracted_map AS (
    SELECT
        unnest(expanded_block) as value,
        f.len_map as len_map,
        CASE when value != '.' then 1 end as test,
        f.free_space
    FROM expanded_map e
        JOIN flattened_map f on e.join_id = f.join_id
),

extracted_map_idx AS (

     SELECT *,
         row_number() over() AS idx
    FROM extracted_map
),

free_space AS (
       SELECT
            *,
           row_number() OVER(partition by test Order by idx desc) as reverse,
           row_number() OVER(partition by value order by idx) as free
           -- this needs to be on alll except '.'
           FROM extracted_map_idx
               order by idx
),

compacted AS (
    SELECT
        CASE WHEN f1.value = '.' THEN f2.value
        ELSE f1.value
        END as value,
        f1.idx -1 as file_id
    FROM free_space f1
    left join free_space f2
    on f1.free = f2.reverse
    where f2.test = 1
    and f1.idx <= f1.len_map - f1.free_space

    ORDER BY f1.idx


)

SELECT SUM(value::BIGINT* file_id) as result
FROM compacted
