-- SmartSales Dashboard Queries

-- 1. Revenue by product category
SELECT product_category, total_revenue, delivered_orders
FROM workspace.smartsales.gold_category_sales
ORDER BY total_revenue DESC;

-- 2. Revenue by city
SELECT city, total_revenue, delivered_orders
FROM workspace.smartsales.gold_city_sales
ORDER BY total_revenue DESC;

-- 3. Monthly revenue
SELECT month, total_revenue
FROM workspace.smartsales.gold_monthly_sales
ORDER BY month;

-- 4. Data quality issues
SELECT data_quality_issue, COUNT(*) AS affected_records
FROM workspace.smartsales.data_quality_issues
GROUP BY data_quality_issue
ORDER BY affected_records DESC;