CREATE OR REPLACE DATABASE RETAIL_DB;

CREATE OR REPLACE SCHEMA RETAIL_SCHEMA;

CREATE OR REPLACE STAGE MY_STAGE;

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = 'JSON';

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

-- RAW LAYER STARTS FROM HERE 

CREATE OR REPLACE TABLE RAW_ORDERS (
    data VARIANT
);

CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    gender STRING,
    date_of_birth DATE,
    age INT,
    country STRING,
    created_at TIMESTAMP
);

CREATE OR REPLACE TABLE RAW_PRODUCTS (
    product_id STRING,
    product_name STRING,
    category STRING,
    price FLOAT,
    currency STRING,
    stock_quantity INT,
    availability_status STRING
);

-- DATA FROM STAGES ARE LOADED TO RAW TABLES HERE

COPY INTO RAW_ORDERS
FROM @MY_STAGE/orders_v2.json
FILE_FORMAT = JSON_FORMAT;

COPY INTO RAW_CUSTOMERS
FROM @MY_STAGE/customers_v2.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO RAW_PRODUCTS
FROM @MY_STAGE/products_v2.csv
FILE_FORMAT = CSV_FORMAT;

-- VALIDATION LAYER STARTS HERE 
-- VALID TABLES ARE CREATED HERE
-- DATA VALIDATION IS DONE HERE

CREATE OR REPLACE TABLE VALID_CUSTOMERS AS
SELECT
    customer_id,

    TRIM(name) AS name,

    COALESCE(NULLIF(TRIM(LOWER(email)), ''), 'unknown@mail.com') AS email,

    COALESCE(NULLIF(TRIM(phone), ''), '0000000000') AS phone,

    CASE 
        WHEN gender IS NULL THEN 'UNKNOWN'
        WHEN TRIM(gender) = '' THEN 'UNKNOWN'
        ELSE UPPER(TRIM(gender))
    END AS gender,

    date_of_birth,

    CASE 
        WHEN age < 18 OR age > 100 THEN 'not elgible'
        ELSE age
    END AS age,

    COALESCE(NULLIF(TRIM(UPPER(country)), ''), 'UNKNOWN') AS country,

    created_at,

    CURRENT_TIMESTAMP() AS load_ts

FROM RAW_CUSTOMERS

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY created_at DESC
) = 1;

CREATE OR REPLACE TABLE VALID_PRODUCTS AS
SELECT DISTINCT
    product_id,
    
    INITCAP(TRIM(product_name)) AS product_name,
    
    COALESCE(category, 'UNKNOWN') AS category,
    
    CASE 
        WHEN price < 0 OR price IS NULL THEN 0
        ELSE price
    END AS price,
    
    currency,
    
    CASE 
        WHEN stock_quantity < 0 OR stock_quantity IS NULL THEN 0
        ELSE stock_quantity
    END AS stock_quantity,
    
    COALESCE(availability_status, 'UNKNOWN') AS availability_status

FROM RAW_PRODUCTS;

CREATE OR REPLACE TABLE VALID_ORDERS AS
SELECT
    f.value:order_id::STRING AS order_id,

    CASE 
        WHEN f.value:customer_id::STRING = 'INVALID' THEN NULL
        ELSE f.value:customer_id::STRING
    END AS customer_id,

    f.value:product_id::STRING AS product_id,

    
    COALESCE(
        CASE 
            WHEN f.value:order_amount::FLOAT < 0 THEN 0
            ELSE f.value:order_amount::FLOAT
        END, 0
    ) AS order_amount,

    f.value:currency::STRING AS currency,

    COALESCE(UPPER(TRIM(f.value:payment_method::STRING)), 'UNKNOWN') AS payment_method,

    
    f.value:shipping_address AS shipping_address,
    COALESCE(f.value:shipping_address.city::STRING, 'UNKNOWN') AS city,

 
    TRY_TO_TIMESTAMP(f.value:order_timestamp::STRING) AS order_timestamp,
    TRY_TO_DATE(f.value:delivery_date::STRING) AS delivery_date,

    TRY_TO_TIMESTAMP(f.value:created_at::STRING) AS created_at,
    TRY_TO_TIMESTAMP(f.value:updated_at::STRING) AS updated_at,

    COALESCE(UPPER(TRIM(f.value:status::STRING)), 'UNKNOWN') AS status,

    CURRENT_TIMESTAMP() AS load_ts

FROM RAW_ORDERS,
     LATERAL FLATTEN(input => data) f

WHERE f.value:order_id IS NOT NULL;

-- CONFORMATION OF DATA CLEANING IS DONE HERE

SELECT * FROM VALID_CUSTOMERS  ;
SELECT * FROM VALID_PRODUCTS ;
SELECT * FROM VALID_ORDERS ;

-- AUTOMATION AND NEW DA VALIDATION IS DONE HERE

CREATE OR REPLACE STREAM ORDERS_STREAM
ON TABLE RAW_ORDERS;

CREATE OR REPLACE STREAM CUSTOMERS_STREAM
ON TABLE RAW_CUSTOMERS;

CREATE OR REPLACE STREAM PRODUCTS_STREAM
ON TABLE RAW_PRODUCTS;

CREATE OR REPLACE TASK ORDERS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS

MERGE INTO ORDERS_CLEAN tgt
USING (

    SELECT
        f.value:order_id::STRING AS order_id,

        -- Remove INVALID customer
        CASE 
            WHEN f.value:customer_id::STRING = 'INVALID' THEN NULL
            ELSE f.value:customer_id::STRING
        END AS customer_id,

        f.value:product_id::STRING AS product_id,

        -- Currency Conversion (USD → INR)
        CASE 
            WHEN f.value:currency::STRING = 'USD' 
                THEN f.value:order_amount::FLOAT * 83   -- conversion rate
            WHEN f.value:order_amount::FLOAT < 0 
                OR f.value:order_amount IS NULL 
                THEN 0
            ELSE f.value:order_amount::FLOAT
        END AS order_amount,

        -- Normalize currency
        'INR' AS currency,

        COALESCE(UPPER(TRIM(f.value:payment_method::STRING)), 'UNKNOWN') AS payment_method,

        f.value:shipping_address AS shipping_address,

        -- Fix timestamps safely
        TRY_TO_TIMESTAMP(f.value:order_timestamp::STRING) AS order_timestamp,
        TRY_TO_DATE(f.value:delivery_date::STRING) AS delivery_date,
        TRY_TO_TIMESTAMP(f.value:created_at::STRING) AS created_at,
        TRY_TO_TIMESTAMP(f.value:updated_at::STRING) AS updated_at,

        COALESCE(UPPER(TRIM(f.value:status::STRING)), 'UNKNOWN') AS status

    FROM ORDERS_STREAM,
         LATERAL FLATTEN(input => data) f

    -- 🚫 Remove invalid records
    WHERE f.value:order_id IS NOT NULL
      AND f.value:product_id IS NOT NULL

) src

ON tgt.order_id = src.order_id

WHEN MATCHED THEN UPDATE SET
    tgt.order_amount = src.order_amount,
    tgt.currency = src.currency,
    tgt.status = src.status,
    tgt.updated_at = src.updated_at

WHEN NOT MATCHED THEN INSERT (
    order_id,
    customer_id,
    product_id,
    order_amount,
    currency,
    payment_method,
    shipping_address,
    order_timestamp,
    delivery_date,
    created_at,
    updated_at,
    status
)
VALUES (
    src.order_id,
    src.customer_id,
    src.product_id,
    src.order_amount,
    src.currency,
    src.payment_method,
    src.shipping_address,
    src.order_timestamp,
    src.delivery_date,
    src.created_at,
    src.updated_at,
    src.status
);

CREATE OR REPLACE TASK CUSTOMERS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS

MERGE INTO VALID_CUSTOMERS tgt
USING (

    SELECT
        customer_id,

        TRIM(name) AS name,

        COALESCE(NULLIF(TRIM(LOWER(email)), ''), 'unknown@mail.com') AS email,

        COALESCE(NULLIF(TRIM(phone), ''), '0000000000') AS phone,

        CASE 
            WHEN gender IS NULL OR TRIM(gender) = '' THEN 'UNKNOWN'
            ELSE UPPER(TRIM(gender))
        END AS gender,

        date_of_birth,

        CASE 
            WHEN age < 18 OR age > 100 THEN NULL
            ELSE age
        END AS age,

        COALESCE(NULLIF(TRIM(UPPER(country)), ''), 'UNKNOWN') AS country,

        created_at,

        CURRENT_TIMESTAMP() AS load_ts

    FROM CUSTOMERS_STREAM

) src

ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN UPDATE SET
    tgt.name = src.name,
    tgt.email = src.email,
    tgt.phone = src.phone,
    tgt.gender = src.gender,
    tgt.age = src.age,
    tgt.country = src.country,
    tgt.created_at = src.created_at,
    tgt.load_ts = src.load_ts

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    name,
    email,
    phone,
    gender,
    date_of_birth,
    age,
    country,
    created_at,
    load_ts
)
VALUES (
    src.customer_id,
    src.name,
    src.email,
    src.phone,
    src.gender,
    src.date_of_birth,
    src.age,
    src.country,
    src.created_at,
    src.load_ts
);

CREATE OR REPLACE TASK PRODUCTS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS

MERGE INTO VALID_PRODUCTS tgt
USING (

    SELECT DISTINCT
        product_id,

        INITCAP(TRIM(product_name)) AS product_name,

        COALESCE(category, 'UNKNOWN') AS category,

        CASE 
            WHEN price < 0 OR price IS NULL THEN 0
            ELSE price
        END AS price,

        currency,

        CASE 
            WHEN stock_quantity < 0 OR stock_quantity IS NULL THEN 0
            ELSE stock_quantity
        END AS stock_quantity,

        COALESCE(availability_status, 'UNKNOWN') AS availability_status

    FROM PRODUCTS_STREAM

) src

ON tgt.product_id = src.product_id

WHEN MATCHED THEN UPDATE SET
    tgt.product_name = src.product_name,
    tgt.category = src.category,
    tgt.price = src.price,
    tgt.stock_quantity = src.stock_quantity,
    tgt.availability_status = src.availability_status

WHEN NOT MATCHED THEN INSERT (
    product_id,
    product_name,
    category,
    price,
    currency,
    stock_quantity,
    availability_status
)
VALUES (
    src.product_id,
    src.product_name,
    src.category,
    src.price,
    src.currency,
    src.stock_quantity,
    src.availability_status
);


ALTER TASK CUSTOMERS_TASK RESUME;
ALTER TASK PRODUCTS_TASK RESUME;
ALTER TASK ORDERS_TASK RESUME;


SELECT * FROM ORDERS_STREAM;

-- FACT_ORDERS TABLE

-- DIM_CUSTOMERS TABLE

CREATE OR REPLACE TABLE DIM_CUSTOMERS AS
SELECT
    customer_id,
    name,
    email,
    gender,
    age,
    country,
    created_at
FROM VALID_CUSTOMERS; 

-- DIM_PRODUCTS TABLE

CREATE OR REPLACE TABLE DIM_PRODUCTS AS
SELECT
    product_id,
    product_name,
    category,
    price,
    currency,
    stock_quantity,
    availability_status
FROM VALID_PRODUCTS;

-- DIM_DATE TABLE

CREATE OR REPLACE TABLE DIM_DATE AS
SELECT DISTINCT
    DATE(order_timestamp) AS date_id,
    
    EXTRACT(YEAR FROM order_timestamp) AS year,
    EXTRACT(MONTH FROM order_timestamp) AS month,
    EXTRACT(DAY FROM order_timestamp) AS day,
    
    DAYNAME(order_timestamp) AS day_name

FROM VALID_ORDERS
WHERE order_timestamp IS NOT NULL;

-- FACT_ORDERS TABLE

CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT
    o.order_id,
    
    o.customer_id,
    o.product_id,
    
    DATE(o.order_timestamp) AS date_id,
    
    o.order_amount,
    o.currency,
    o.payment_method,
    o.status,
    
    o.order_timestamp,
    o.delivery_date

FROM VALID_ORDERS o
WHERE o.order_timestamp IS NOT NULL;

-- KPI SECTION 

SELECT 
    SUM(order_amount) AS total_revenue
FROM FACT_ORDERS
WHERE status = 'COMPLETED';

SELECT
    customer_id,
    SUM(order_amount) AS customer_lifetime_value
FROM FACT_ORDERS
WHERE status = 'COMPLETED'
GROUP BY customer_id
ORDER BY customer_lifetime_value DESC;

SELECT
    p.product_name,
    COUNT(*) AS total_orders,
    SUM(f.order_amount) AS revenue
FROM FACT_ORDERS f
JOIN DIM_PRODUCTS p
ON f.product_id = p.product_id
WHERE f.status = 'COMPLETED'
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;

SELECT
    date_id,
    SUM(order_amount) AS daily_revenue
FROM FACT_ORDERS
WHERE status = 'COMPLETED'
GROUP BY date_id
ORDER BY date_id;

SELECT
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) * 100.0 /
    COUNT(*) AS conversion_rate
FROM FACT_ORDERS;

SELECT
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) * 100.0 /
    COUNT(*) AS cart_abandonment_rate
FROM FACT_ORDERS;

SELECT
    c.country,
    SUM(f.order_amount) AS revenue
FROM FACT_ORDERS f
JOIN DIM_CUSTOMERS c
ON f.customer_id = c.customer_id
WHERE f.status = 'COMPLETED'
GROUP BY c.country
ORDER BY revenue DESC;

SELECT
    payment_method,
    COUNT(*) AS total_orders,
    SUM(order_amount) AS revenue
FROM FACT_ORDERS
GROUP BY payment_method
ORDER BY revenue DESC;

SELECT
    AVG(DATEDIFF('day', order_timestamp, delivery_date)) AS avg_delivery_days
FROM FACT_ORDERS
WHERE delivery_date IS NOT NULL;