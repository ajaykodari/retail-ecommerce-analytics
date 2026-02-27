-- ============================================================
-- RETAIL / E-COMMERCE ANALYTICS DASHBOARD
-- File 2: Data Extraction Queries (for Python / Power BI)
-- Author: Ajay Kodari
-- ============================================================

USE retail_analytics;

-- ============================================================
-- QUERY 1: MASTER SALES FACT TABLE
-- (Main table to load into Python for cleaning)
-- ============================================================
SELECT
    o.order_id,
    o.order_date,
    o.ship_date,
    o.ship_mode,
    o.region,
    YEAR(o.order_date)                                      AS order_year,
    MONTH(o.order_date)                                     AS order_month,
    MONTHNAME(o.order_date)                                 AS month_name,
    QUARTER(o.order_date)                                   AS order_quarter,
    DATEDIFF(o.ship_date, o.order_date)                     AS shipping_days,
    c.customer_id,
    c.customer_name,
    c.gender,
    c.age,
    c.city,
    c.state,
    c.segment,
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    p.cost_price,
    oi.quantity,
    oi.unit_price                                           AS selling_price,
    oi.discount,
    ROUND(oi.unit_price * (1 - oi.discount), 2)            AS net_price,
    ROUND(oi.quantity * oi.unit_price * (1 - oi.discount), 2) AS revenue,
    ROUND(oi.quantity * p.cost_price, 2)                   AS total_cost,
    ROUND(
        (oi.quantity * oi.unit_price * (1 - oi.discount))
        - (oi.quantity * p.cost_price), 2
    )                                                       AS profit,
    ROUND(
        ((oi.unit_price * (1 - oi.discount)) - p.cost_price) / p.cost_price * 100, 2
    )                                                       AS profit_margin_pct,
    CASE WHEN r.order_id IS NOT NULL THEN 'Returned' ELSE 'Completed' END AS order_status
FROM orders o
JOIN customers c  ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p   ON oi.product_id = p.product_id
LEFT JOIN returns r ON o.order_id = r.order_id
ORDER BY o.order_date;

-- ============================================================
-- QUERY 2: CUSTOMER LIFETIME VALUE (CLV)
-- ============================================================
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    c.city,
    c.state,
    c.gender,
    c.age,
    COUNT(DISTINCT o.order_id)                              AS total_orders,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS total_revenue,
    ROUND(AVG(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS avg_order_value,
    MIN(o.order_date)                                       AS first_order_date,
    MAX(o.order_date)                                       AS last_order_date,
    DATEDIFF(MAX(o.order_date), MIN(o.order_date))          AS customer_lifespan_days,
    ROUND(
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount)) /
        NULLIF(COUNT(DISTINCT o.order_id), 0), 2
    )                                                       AS clv_estimate
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.customer_name, c.segment, c.city, c.state, c.gender, c.age
ORDER BY clv_estimate DESC;

-- ============================================================
-- QUERY 3: MONTHLY SALES TREND (YoY Comparison)
-- ============================================================
SELECT
    YEAR(o.order_date)     AS year,
    MONTH(o.order_date)    AS month,
    MONTHNAME(o.order_date) AS month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity)       AS total_units_sold,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS total_revenue,
    ROUND(SUM((oi.quantity * oi.unit_price * (1 - oi.discount)) - (oi.quantity * p.cost_price)), 2) AS total_profit
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY YEAR(o.order_date), MONTH(o.order_date), MONTHNAME(o.order_date)
ORDER BY year, month;

-- ============================================================
-- QUERY 4: PRODUCT PERFORMANCE
-- ============================================================
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    SUM(oi.quantity)       AS total_units_sold,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS total_revenue,
    ROUND(SUM((oi.quantity * oi.unit_price * (1 - oi.discount)) - (oi.quantity * p.cost_price)), 2) AS total_profit,
    ROUND(
        SUM((oi.quantity * oi.unit_price * (1 - oi.discount)) - (oi.quantity * p.cost_price)) /
        NULLIF(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 0) * 100, 2
    )                      AS profit_margin_pct
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category, p.sub_category, p.brand
ORDER BY total_revenue DESC;

-- ============================================================
-- QUERY 5: REGIONAL PERFORMANCE
-- ============================================================
SELECT
    o.region,
    c.state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS total_revenue,
    ROUND(SUM((oi.quantity * oi.unit_price * (1 - oi.discount)) - (oi.quantity * p.cost_price)), 2) AS total_profit
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY o.region, c.state
ORDER BY total_revenue DESC;

-- ============================================================
-- QUERY 6: CUSTOMER SEGMENTATION (RFM Base)
-- ============================================================
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    DATEDIFF('2024-12-31', MAX(o.order_date))               AS recency_days,
    COUNT(DISTINCT o.order_id)                              AS frequency,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS monetary
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.customer_name, c.segment
ORDER BY monetary DESC;

-- ============================================================
-- QUERY 7: TOP 10 CUSTOMERS BY REVENUE
-- ============================================================
SELECT
    c.customer_name,
    c.segment,
    c.city,
    COUNT(DISTINCT o.order_id)  AS total_orders,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS total_revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.customer_name, c.segment, c.city
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================================
-- QUERY 8: CATEGORY REVENUE SHARE
-- ============================================================
SELECT
    p.category,
    COUNT(DISTINCT o.order_id)  AS total_orders,
    SUM(oi.quantity)            AS total_units,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS total_revenue,
    ROUND(SUM((oi.quantity * oi.unit_price * (1 - oi.discount)) - (oi.quantity * p.cost_price)), 2) AS total_profit,
    ROUND(
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount)) /
        (SELECT SUM(oi2.quantity * oi2.unit_price * (1 - oi2.discount)) FROM order_items oi2) * 100, 2
    )                           AS revenue_share_pct
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
GROUP BY p.category
ORDER BY total_revenue DESC;
