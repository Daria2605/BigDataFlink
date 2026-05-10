CREATE SCHEMA IF NOT EXISTS star;

CREATE TABLE IF NOT EXISTS star.dim_customer (
    customer_key TEXT PRIMARY KEY,
    source_customer_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_seller (
    seller_key TEXT PRIMARY KEY,
    source_seller_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_supplier (
    supplier_key TEXT PRIMARY KEY,
    supplier_name TEXT NOT NULL,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_product (
    product_key TEXT PRIMARY KEY,
    source_product_id BIGINT,
    product_name TEXT,
    product_category TEXT,
    pet_category TEXT,
    price NUMERIC(12,2),
    stock_quantity INT,
    weight NUMERIC(12,2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3,1),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_key TEXT,
    supplier_name TEXT,
    supplier_email TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_store (
    store_key TEXT PRIMARY KEY,
    store_name TEXT NOT NULL,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT
);

CREATE TABLE IF NOT EXISTS star.fact_sales (
    sale_event_id TEXT PRIMARY KEY,
    source_file TEXT,
    source_row_id BIGINT,
    sale_date DATE,
    customer_key TEXT REFERENCES star.dim_customer(customer_key),
    seller_key TEXT REFERENCES star.dim_seller(seller_key),
    product_key TEXT REFERENCES star.dim_product(product_key),
    store_key TEXT REFERENCES star.dim_store(store_key),
    supplier_key TEXT REFERENCES star.dim_supplier(supplier_key),
    sale_quantity INT,
    sale_total_price NUMERIC(12,2),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_date ON star.fact_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON star.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON star.fact_sales(product_key);
