# E-Commerce-Logistics Data Analysis

This project uses dataset from **Brazilian E-Commerce Public Dataset by Olist** [(sources: Kaggle / Olist Store)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_orders_dataset.csv). The dataset has information of 100k online e-commerce orders from 2016 to 2018 made at multiple marketplaces in Brazil. 

## Freight Performance Analysis
Tools: SQL, Tableau

- Create view by joining 2 tables to extract necessary data
```
CREATE OR REPLACE VIEW order_info AS
SELECT 
    o.order_id,
	o.order_approved_at,
	i.shipping_limit_date,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
	o.order_status,
    SUM(i.price) AS total_price,
    SUM(i.freight_value) AS total_freight_cost,
    SUM(i.price) + SUM(i.freight_value) AS order_value
FROM 
    orders o
JOIN 
    item i ON o.order_id = i.order_id
GROUP BY 
    o.order_id,
	o.order_approved_at,
	i.shipping_limit_date,
	o.order_status,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date;
```
- Realize freight performance for each month
```
SELECT
	EXTRACT(YEAR from order_approved_at) as year,
	EXTRACT(MONTH from order_approved_at) as month,
	COUNT(*) AS total_order,
	COUNT(CASE WHEN order_delivered_customer_date < order_estimated_delivery_date THEN order_id END) AS delivery_ontime,
	COUNT(CASE WHEN order_delivered_customer_date>order_estimated_delivery_date THEN order_id END) AS late_delivery
FROM order_info
WHERE (order_status != 'unavailable' OR order_status != 'canceled')
GROUP BY year,month
ORDER BY year ASC,month ASC;
```






