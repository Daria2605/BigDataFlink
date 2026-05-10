SELECT 'dim_customer' AS table_name, COUNT(*) AS rows_count FROM star.dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM star.dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM star.dim_product
UNION ALL SELECT 'dim_store', COUNT(*) FROM star.dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM star.dim_supplier
UNION ALL SELECT 'fact_sales', COUNT(*) FROM star.fact_sales
ORDER BY table_name;

SELECT
    p.product_category,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS sold_items,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM star.fact_sales f
JOIN star.dim_product p ON p.product_key = f.product_key
GROUP BY p.product_category
ORDER BY revenue DESC
LIMIT 10;

SELECT
    c.country,
    COUNT(*) AS sales_count,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM star.fact_sales f
JOIN star.dim_customer c ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY revenue DESC
LIMIT 10;
