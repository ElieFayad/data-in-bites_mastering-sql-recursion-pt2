USE DATABASE RECURSIVE_CTE;
USE SCHEMA RECURSIVE_CTE_DATA;

-- Upload the JSON files in the directory /datasets/ to the snowflake stage 'RECURSIVE_DATA_STAGE'
-- You can either upload them manually through Snowflake's UI: https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui#uploading-files-onto-a-stage
-- or following the steps in this link: https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage

-- list all the files in the stage to make sure that the upload was successful
ls @RECURSIVE_CTE.RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE;

-- query the data in the files
SELECT $1 as Raw_Data
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/car_parts_article_dataset_recursive_demo_w_cycles.json
;

SELECT $1 as Raw_Data
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/car_parts_dataset_recursive_demo_w_cycles.json
;

SELECT $1 as Raw_Data
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/car_parts_large_dataset_recursive_demo_no_cycles.json
;

-- Copy the data into Snowflake tables
COPY INTO RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES_ARTICLE
FROM (SELECT 
        $1:Part_ID::NUMBER 
        , $1:Part_Name::VARCHAR 
        , $1:Part_Category::VARCHAR 
        , $1:Part_Cost::NUMBER(38, 2)
        , $1:Part_Manufacturing_Duration::NUMBER(38, 2) 
        , $1:Depends_On::NUMBER 
      FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/car_parts_article_dataset_recursive_demo_w_cycles.json
);

COPY INTO RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES
FROM (SELECT 
        $1:Part_ID::NUMBER 
        , $1:Part_Name::VARCHAR 
        , $1:Part_Category::VARCHAR 
        , $1:Part_Cost::NUMBER(38, 2)
        , $1:Part_Manufacturing_Duration::NUMBER(38, 2) 
        , $1:Depends_On::NUMBER 
      FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/car_parts_dataset_recursive_demo_w_cycles.json
);

COPY INTO RECURSIVE_CTE_DATA.VEHICLE_PARTS_LARGE_DATASET
FROM (SELECT 
        $1:Part_ID::NUMBER 
        , $1:Part_Name::VARCHAR 
        , $1:Part_Category::VARCHAR 
        , $1:Part_Cost::NUMBER(38, 2)
        , $1:Part_Manufacturing_Duration::NUMBER(38, 2) 
        , $1:Depends_On::NUMBER 
      FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/car_parts_large_dataset_recursive_demo_no_cycles.json
);

-- Make sure data is loaded correctly in the tables
SELECT *
FROM RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES_ARTICLE; -- 7 records

SELECT *
FROM RECURSIVE_CTE_DATA.VEHICLE_PARTS_W_CYCLES; -- 100 records

SELECT *
FROM RECURSIVE_CTE_DATA.VEHICLE_PARTS_LARGE_DATASET; -- 10k records