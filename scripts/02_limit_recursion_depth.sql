USE DATABASE RECURSIVE_CTE;
USE SCHEMA RECURSIVE_CTE_DATA;

-- BoM on article dataset
WITH 
    RECURSIVE PARTS_BOM AS (
        -- Anchor Member: Start with all parts (even those that have dependencies)
        SELECT
            VP.PART_ID,
            VP.PART_NAME,
            VP.PART_CATEGORY,
            VP.PART_ID::VARCHAR AS BOM,
            1 AS RECURSION_DEPTH
        FROM 
            RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES_ARTICLE VP

        UNION ALL

        -- Recursive Member: Traverse the parts that depend on others
        SELECT
            VP.PART_ID,
            VP.PART_NAME,
            VP.PART_CATEGORY,
            PB.BOM || '->' || VP.PART_ID::VARCHAR AS BOM,
            PB.RECURSION_DEPTH + 1 AS RECURSION_DEPTH
        FROM 
            RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES_ARTICLE VP
            INNER JOIN PARTS_BOM PB
                ON VP.DEPENDS_ON = PB.PART_ID
        WHERE
            -- Avoid infinite recursion
            -- Loop at most 10 times
            PB.RECURSION_DEPTH < 10  
)
-- Final Select: Retrieve the BOM for all parts
SELECT
    PART_ID,
    PART_NAME,
    PART_CATEGORY,
    BOM,
    RECURSION_DEPTH
FROM PARTS_BOM
ORDER BY PART_ID;

-- BoM on dataset with cycles
WITH 
    RECURSIVE PARTS_BOM AS (
        -- Anchor Member: Start with all parts (even those that have dependencies)
        SELECT
            VP.PART_ID,
            VP.PART_NAME,
            VP.PART_CATEGORY,
            VP.PART_ID::VARCHAR AS BOM,
            1 AS RECURSION_DEPTH
        FROM 
            RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES VP

        UNION ALL

        -- Recursive Member: Traverse the parts that depend on others
        SELECT
            VP.PART_ID,
            VP.PART_NAME,
            VP.PART_CATEGORY,
            PB.BOM || '->' || VP.PART_ID::VARCHAR AS BOM,
            PB.RECURSION_DEPTH + 1 AS RECURSION_DEPTH
        FROM 
            RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES VP
            INNER JOIN PARTS_BOM PB
                ON VP.DEPENDS_ON = PB.PART_ID
        WHERE
            -- Avoid infinite recursion
            -- Loop at most 1000 times
            PB.RECURSION_DEPTH < 1000  
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
ORDER BY PART_ID;

-- BoM on large dataset
WITH 
    RECURSIVE PARTS_BOM AS (
        -- Anchor Member: Start with all parts (even those that have dependencies)
        SELECT
            VP.PART_ID,
            VP.PART_NAME,
            VP.PART_CATEGORY,
            VP.PART_ID::VARCHAR AS BOM,
            1 AS RECURSION_DEPTH
        FROM 
            RECURSIVE_CTE_DATA.VEHICLE_PARTS_LARGE_DATASET VP

        UNION ALL

        -- Recursive Member: Traverse the parts that depend on others
        SELECT
            VP.PART_ID,
            VP.PART_NAME,
            VP.PART_CATEGORY,
            PB.BOM || '->' || VP.PART_ID::VARCHAR AS BOM,
            PB.RECURSION_DEPTH + 1 AS RECURSION_DEPTH
        FROM 
            RECURSIVE_CTE_DATA.VEHICLE_PARTS_LARGE_DATASET VP
            INNER JOIN PARTS_BOM PB
                ON VP.DEPENDS_ON = PB.PART_ID
        WHERE
            -- Avoid infinite recursion
            -- Loop at most 1000 times
            PB.RECURSION_DEPTH < 1000  
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
ORDER BY PART_ID;