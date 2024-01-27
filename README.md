# Logistics Data Analysis

This project uses dataset from **Brazilian E-Commerce Public Dataset by Olist** [(sources: Kaggle / Olist Store)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_orders_dataset.csv). The dataset has information of 100k online e-commerce orders from 2016 to 2018 made at multiple marketplaces in Brazil. 

## Freight-Shipping Performance Analysis
Tools: `SQL`, `Tableau`

**Goal for this analysis**: Identify negative performance from previous logistics data to explore areas of improvement, and develop actionable strategies.

A summarized techniques I used in SQL:
- Cleaned and transformmed data, joined 4 tables and grouped to extract necessary data to use in Tableau.
- Windows functions / CASE / CTEs / Customized Function / Ranking
- Calculating delta, running totals and Date-Time manipulation.


### Create an order information to extract necessary data
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
### Realize freight performance on each month
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
<img width="500" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/21807168-4103-432f-8463-7a6ab34dbb89">
<img width="876" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/4fa7458b-9221-46e9-80a0-56e94822db6e">


### Check the duty for late delivery (seller or carrier)
```
SELECT
	EXTRACT(YEAR from order_approved_at) as year,
	EXTRACT(MONTH from order_approved_at) as month,
	COUNT(*) AS total_order,
	COUNT(CASE WHEN order_delivered_customer_date>order_estimated_delivery_date THEN order_id END) AS late_to_customer,
	COUNT(CASE WHEN 
		  	shipping_limit_date < order_delivered_carrier_date AND order_delivered_customer_date > order_estimated_delivery_date 
		  THEN order_id END) AS late_by_seller,
	COUNT(CASE WHEN 
		  	shipping_limit_date > order_delivered_carrier_date AND order_delivered_customer_date > order_estimated_delivery_date 
		  THEN order_id END) AS late_by_carrier
FROM order_info
WHERE (order_status != 'unavailable' OR order_status != 'canceled')
GROUP BY year,month
ORDER BY year ASC,month ASC;
```
<img width="812" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/f5df1057-691b-4f61-abc0-e4277c26af20">

Late delivery could be casued by two reasons:
- Seller delayed on giving products to carrier
- Carrier delayed on transportation

From the chart I realize more delays are caused by the seller -- It's important to take action to maintain our platform's reputation and customer loyalty. My recommended strategy:
<img width="1069" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/486cb469-d5c3-4dda-b6ab-c0b826ab7980">


### Realize Freight Cost

Here, I created a customized indicator -- **`volumn_weight`** -- to represent the interactive relationship between volumn and weight, both are crucially in affecting the freight cost.

`volumn_weight = ((width * length * height) / weight) / 100000`
```
CREATE OR REPLACE VIEW freight_info AS
SELECT 
	EXTRACT(YEAR from order_approved_at) as year,
	EXTRACT(MONTH from order_approved_at) as month,
	oi.order_id,
	ROUND(AVG(((product_width_cm * product_length_cm * product_height_cm) * product_weight_g)/100000),2) AS volumn_weight,
	SUM(total_price)AS total_price,
	SUM(total_freight_cost)AS total_freight_cost,
	SUM(order_value)AS total_order_value,
	SUM(total_price - total_freight_cost)  AS net_sale
FROM item AS i
JOIN order_info AS oi on oi.order_id = i.order_id
JOIN product AS p on p.product_id = i.product_id
WHERE (order_status != 'unavailable' OR order_status != 'canceled')
GROUP BY year,month,oi.order_id
ORDER BY year ASC, month ASC,oi.order_id ASC;
```
### Create report to monitor freight cost-efficiency

The **`freight_ratio`** defined as `freight_cost/volumn_weight`, meaning the higher the ratio, the more products we shipped under the same cost.

Freight_ratio performed better during first quarter in 2017, while it was trending down since then until recently. Maintaing the Freight_ratio above `3` can ensure the efficiency, and my suggestion is:
- Working with logistics team to realize the cost of each transportation type (railway, bus, and air...)
- Negotiate with carrier to prevent fluncuate freight cost, and possibly getting better deal with long-term business relationship
- Consolidate more orders into one shipment to optimize shipping efficiency.

```
CREATE VIEW freight_ratio AS
SELECT 
	year,
	month,
	COUNT(*) AS total_order,
	SUM(total_freight_cost) AS total_freight_cost,
	ROUND(AVG(total_freight_cost/volumn_weight),2) AS freight_ratio 
FROM freight_info
WHERE total_freight_cost !=0 AND volumn_weight !=0
GROUP BY year,month;
```
<img width="915" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/06c01576-a256-4954-b1d8-f4e17454e9c3">

### Comparing each month's freight cost to last month

This report can be further modified to tracking weekly freight cost to monitor the trending, allowing logistics team to adjust strategies if needed.

```
SELECT
    year,
    month,
    SUM(total_freight_cost) AS total_freight_cost,
    SUM(total_freight_cost) - LAG(SUM(total_freight_cost), 1, 0) OVER (PARTITION BY year ORDER BY month ASC) AS delta,
    SUM(SUM(total_freight_cost)) OVER (PARTITION BY year ORDER BY month ASC) AS cumulative_freight_cost
FROM freight_info
WHERE year BETWEEN 2017 AND 2018
GROUP BY year, month
ORDER BY year ASC, month ASC;
```

### Create customized Function to categorize freight ratio

This methods helps to create category that can be useful for other analysis, such as using logistis analysis to predict the future freight_ratio performance.

```
CREATE OR REPLACE FUNCTION get_freight_info(freight_ratio numeric) RETURNS text AS $$
BEGIN
    RETURN CASE
        WHEN freight_ratio < 2 THEN 'Bad'
        WHEN freight_ratio BETWEEN 2 AND 2.5 THEN 'Low'
        WHEN freight_ratio BETWEEN 2.5 AND 3 THEN 'Normal'
        WHEN freight_ratio BETWEEN 3 AND 3.5 THEN 'Good'
        WHEN freight_ratio > 3.5 THEN 'Excellent'
    END;
END;
$$ LANGUAGE plpgsql;

SELECT year,month,freight_ratio
FROM freight_ratio;

SELECT year,month,get_freight_info(freight_ratio)
FROM freight_ratio;
```
<img width="308" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/f6f45a60-ad20-42e4-b6ee-f8e6786d992e">
<img width="350" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/07b1635e-d110-40c2-8618-65a30966e4cb">

### Ranking the total freight Cost of each product category

This report helps to identify which category are having a higher percentage in total freight cost, enabling team to do additional analysis on specific category.
```
	-- 
SELECT 
	product_category_name_english,
	SUM(freight_value) AS total_freight_cost,
	RANK()OVER(ORDER BY SUM(freight_value) DESC) AS rank_total_freight_cost
FROM item AS i
JOIN Product AS p on i.product_id = p.product_id
JOIN orders AS o on i.order_id = o.order_id
JOIN category_translate AS ct on p.product_category_name = ct.product_category_name
WHERE EXTRACT(Year FROM order_approved_at) BETWEEN 2017 AND 2018
GROUP BY product_category_name_english
ORDER BY rank_total_freight_cost ASC;
```
<img width="548" alt="image" src="https://github.com/leonlin97/E-Commerce-Logistics/assets/142073522/d96f87c7-fc5c-4fb6-83f8-aa053452988d">









