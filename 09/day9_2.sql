WITH day9 AS (
    SELECT
        string_split_regex(column0, '') AS disk_map
    FROM read_csv_auto('test.csv', all_varchar=true, header = false, delim = '')
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
           row_number() OVER(partition by value order by idx) as free,
            CASE WHEN value != '.' THEN
                9 - value::INTEGER
            ELSE NULL
            END AS reverse_file_idx
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


),

part2_prep AS (
        SELECT list(value order by idx), any_value(free_space) as free_space, any_value(len_map) as len_map
        FROM free_space
),

part2 AS (
     WITH RECURSIVE ctx(full_map, free_space, start_idx, end_idx) AS (
        SELECT full_map, len(full_map), 1, len(full_map)
        FROM flattened_map

        UNION ALL
        -- calculate the next space
        -- find the next disk id that fits in it
        -- if it fits change the array
       -- until no disk id fits any more into the free spaces

        SELECT
            CASE WHEN full_map[start_idx] = '.' THEN
                full_map[1:start_idx-1] || [full_map[end_idx]] || full_map[start_idx+1:len(full_map)-1] || ['.']
            ELSE
                full_map
            END AS full_map,
            free_space - 1,
            CASE WHEN full_map[start_idx] != '.' THEN
                start_idx + 1
            ELSE start_idx
            END AS start_idx,
            CASE WHEN full_map[end_idx] = '.' THEN
                end_idx - 1
                WHEN full_map[start_idx] = '.' THEN
                end_idx -1
            ELSE end_idx
            END AS end_idx
        FROM ctx
        WHERE
            start_idx <= end_idx

    )
    SELECT full_map[1:end_idx] as full_map
    FROM ctx
        WHERE start_idx = end_idx
)

SELECT *
FROM part2_prep
