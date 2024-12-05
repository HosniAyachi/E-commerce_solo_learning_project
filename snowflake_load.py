import snowflake.connector

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user='HosniAyachi',
    password='*****',
    account='ww75523.europe-west2.gcp',
    warehouse='ecommerce_wh',
    database='ecommerce_db',
    schema='ecommerce_schema'
)
cursor = conn.cursor()
cursor.execute("USE ROLE ecommerce_role")
cursor.execute("USE WAREHOUSE ecommerce_wh")
cursor.execute("USE DATABASE ecommerce_db")
cursor.execute("USE SCHEMA ecommerce_schema")
# Stage file
cursor.execute("""
    CREATE  STAGE IF NOT EXISTS ecommerce_stage
""")
# Customers
cursor.execute("""
    CREATE OR REPLACE TABLE customers (
        customer_id STRING,
        customer_unique_id STRING,
        customer_zip_code_prefix INT,
        customer_city STRING,
        customer_state STRING
    )
""")
cursor.execute("""
    PUT file://data_output/customers.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO customers
    FROM @ecommerce_stage/customers.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

# Geolocation
cursor.execute("""
    CREATE OR REPLACE TABLE geolocation (
        geolocation_zip_code_prefix INT,
        geolocation_lat DECIMAL(9,6),
        geolocation_lng DECIMAL(9,6),
        geolocation_city STRING,
        geolocation_state STRING
    )
""")
cursor.execute("""
    PUT file://data_output/geolocation.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO geolocation
    FROM @ecommerce_stage/geolocation.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

# Order_items
cursor.execute("""
    CREATE OR REPLACE TABLE order_items (
        order_id STRING,
        order_item_id STRING,
        product_id STRING,
        seller_id STRING,
        shipping_limit_date DATE,
        price FLOAT,
        freight_value FLOAT
    )
""")
cursor.execute("""
    PUT file://data_output/order_items.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO order_items
    FROM @ecommerce_stage/order_items.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

# Order_payments
cursor.execute("""
    CREATE OR REPLACE TABLE order_payments (
        order_id STRING,
        payment_sequential INT,
        payment_type STRING,
        payment_installments INT,
        payment_value FLOAT
    )
""")
cursor.execute("""
    PUT file://data_output/order_payments.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO order_payments
    FROM @ecommerce_stage/order_payments.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

# Order_reviews
cursor.execute("""
    CREATE OR REPLACE TABLE order_reviews (
        review_id STRING,
        order_id STRING,
        review_score INT,
        review_creation_date DATE,
        review_answer_timestamp DATE
    )
""")
cursor.execute("""
    PUT file://data_output/order_reviews.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO order_reviews
    FROM @ecommerce_stage/order_reviews.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

# Orders
cursor.execute("""
    CREATE OR REPLACE TABLE orders (
        order_id STRING,
        customer_id STRING,
        order_status STRING,
        order_purchase_timestamp DATE,
        order_estimated_delivery_date DATE
    )
""")
cursor.execute("""
    PUT file://data_output/orders.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO orders
    FROM @ecommerce_stage/orders.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

# Products
cursor.execute("""
    CREATE OR REPLACE TABLE products (
        product_id STRING,
        product_category_name STRING,
        product_name_lenght FLOAT,
        product_description_lenght FLOAT,
        product_photos_qty FLOAT,
        product_weight_g FLOAT,
        product_length_cm FLOAT,
        product_height_cm FLOAT,
        product_width_cm FLOAT
    )
""")
cursor.execute("""
    PUT file://data_output/products.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO products
    FROM @ecommerce_stage/products.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

# Seller
cursor.execute("""
    CREATE OR REPLACE TABLE seller (
        seller_id STRING,
        seller_zip_code_prefix INT,
        seller_city STRING,
        seller_state STRING
    )
""")
cursor.execute("""
    PUT file://data_output/seller.csv @ecommerce_stage
""")
cursor.execute("""
    COPY INTO seller
    FROM @ecommerce_stage/seller.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
""")

cursor.close()
conn.close()
