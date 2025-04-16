USE DATABASE RECURSIVE_CTE;
USE SCHEMA RECURSIVE_CTE_DATA;

-- BoM using Snowflake's CONNECT BY
-- Try it out for the other datasets!
WITH
    PARTS_BOM AS (
        SELECT
            VP.PART_ID,
            VP.PART_NAME,
            VP.PART_CATEGORY,
            SYS_CONNECT_BY_PATH(VP.PART_ID, ' -> ') BOM,
            LEVEL AS RECURSION_DEPTH
        FROM 
            RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES_ARTICLE VP
            START WITH TRUE -- Anchor Member: Start with all parts (even those that have dependencies)
            CONNECT BY 
                -- Recursive Member: Traverse the parts that depend on others
                VP.DEPENDS_ON = PRIOR VP.PART_ID
                -- Avoid infinite recursion
                -- Loop at most 10 times
                AND PRIOR RECURSION_DEPTH < 10
                -- If the list of visited_nodes in the previous iteration does not include the current ID => execute iteration
                -- You could also define an array field just like in the recursive CTE example
                AND PRIOR BOM NOT LIKE '%' || VP.PART_ID || '%'
)
-- Final Select: Retrieve the BOM for all parts
SELECT
    PART_ID,
    PART_NAME,
    PART_CATEGORY,
    BOM,
    RECURSION_DEPTH
FROM PARTS_BOM
-- take the row with maximum recursion depth for each part_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY PART_ID ORDER BY RECURSION_DEPTH DESC) = 1
ORDER BY PART_ID, RECURSION_DEPTH
;