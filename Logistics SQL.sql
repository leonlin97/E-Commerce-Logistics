-- Create table
CREATE TABLE location (
	location_id SERIAL PRIMARY KEY,
	geolocation_zip_code_prefix VARCHAR,
	geolocation_lat VARCHAR,
	geolocation_lng VARCHAR,
	geolocation_city VARCHAR,
	geolocation_state VARCHAR
);
	-- alter location table to remove duplicated zip code
CREATE TABLE location_edit AS
SELECT geolocation_zip_code_prefix,geolocation_city,geolocation_state
FROM (
	SELECT geolocation_zip_code_prefix,geolocation_city,geolocation_state,
	ROW_NUMBER() OVER(PARTITION BY geolocation_zip_code_prefix ) AS num
	FROM location
)
WHERE num = 1
ORDER BY geolocation_zip_code_prefix ASC;

ALTER TABLE location_edit ADD PRIMARY KEY (geolocation_zip_code_prefix);
	--
INSERT INTO location_edit (geolocation_zip_code_prefix,geolocation_city,geolocation_state)
VALUES ('72300','NA','NA');

CREATE TABLE Customer (
	customer_id VARCHAR PRIMARY KEY,
	customer_unique_id VARCHAR,
	customer_zip_code_prefix VARCHAR REFERENCES location_edit(geolocation_zip_code_prefix),
	customer_city VARCHAR,
	customer_state VARCHAR
);

CREATE TABLE Seller (
	seller_id VARCHAR PRIMARY KEY,
	seller_zip_code_prefix VARCHAR REFERENCES location(geolocation_zip_code_prefix),
	seller_city VARCHAR,
	seller_state VARCHAR
);

CREATE TABLE Category_Translate (
	product_category_name VARCHAR PRIMARY KEY,
	product_category_name_english VARCHAR
);

CREATE TABLE Product (
	product_id VARCHAR PRIMARY KEY,
	product_category_name VARCHAR REFERENCES Category_Translate(product_category_name),
	product_name_length int,
	product_description_length int,
	product_photos_qty int,
	product_weight_g NUMERIC(10,2),
	product_length_cm NUMERIC(10,2),
	product_height_cm NUMERIC(10,2)
);
ALTER TABLE Product ADD COLUMN product_width_cm NUMERIC(10,2);

CREATE TABLE Orders (
	order_id VARCHAR PRIMARY KEY,
	customer_id VARCHAR REFERENCES Customer(customer_id),
	order_status VARCHAR,
	order_purchase_timestamp TIMESTAMP,
	order_approved_at TIMESTAMP,
	order_delivered_carrier_date TIMESTAMP,
	order_delivered_customer_date TIMESTAMP,
	order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE Item (
	order_id VARCHAR REFERENCES Orders(order_id),
	order_item_id int,
	product_id VARCHAR REFERENCES Product(product_id),
	seller_id VARCHAR REFERENCES Seller(seller_id),
	shipping_limit_date TIMESTAMP,
	price NUMERIC(10,2),
	freight_value NUMERIC(10,2)
);

CREATE TABLE Payment (
	order_id VARCHAR REFERENCES Orders(order_id),
	payment_sequential int,
	payment_type VARCHAR,
	payment_installments int,
	payment_value NUMERIC(10,2)
);

CREATE TABLE Review (
	review_id VARCHAR,
	order_id VARCHAR REFERENCES Orders(order_id),
	review_score int,
	review_comment_title VARCHAR,
	review_comment_message VARCHAR,
	review_creation_date TIMESTAMP,
	review_answer_timestamp TIMESTAMP
);

-- Import csv file into each table to finish the database creation.

-- Data Analysis
	-- Realizing the delivery performance of each order
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

SELECT * FROM order_info;

	-- check delivery performance
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

	-- check the delay order (seller to carrier and carrier to buyer)
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

	-- check freight cost
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

SELECT * FROM freight_info;


	-- create report for monitoring freight cost-efficiency
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

SELECT * FROM freight_ratio;

	-- Compare each month's freight cost to last month
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

	-- Create customized Function to categorize freight ratio
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

SELECT year,month,get_freight_info(freight_ratio)
FROM freight_ratio;

	-- Rank the total freight Cost of product category
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
