-- this was my first attempt, it works but takes 20 minutes
-- the new option of using a join only takes 0.2 seconds
compacted AS (
    WITH RECURSIVE ctx(full_map, free_space, start_idx, end_idx) AS (
        SELECT full_map, len(full_map), 1, len(full_map)
        FROM flattened_map

        UNION ALL

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
),