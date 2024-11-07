-- DROP TABLE IF EXISTS Dim_Date;
-- DROP TABLE IF EXISTS Dim_Product;
-- DROP TABLE IF EXISTS Dim_Location;
-- DROP TABLE IF EXISTS Dim_Reference_Domain;
-- DROP TABLE IF EXISTS Dim_Browser;
-- DROP TABLE IF EXISTS Fact_View;

CREATE TABLE IF NOT EXISTS Dim_Date (
	datetime_key INT PRIMARY KEY,
	full_date DATE,
	day_of_week VARCHAR(10),
	day_of_week_short VARCHAR(10),
	hour INT,
	day_of_month INT,
	month INT,
	year INT
);


CREATE TABLE IF NOT EXISTS Dim_Product (
	product_key INT PRIMARY KEY,
	product_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Dim_Location (
	location_key INT PRIMARY KEY,
	country_iso2 VARCHAR(20),
	country_iso3 VARCHAR(20),
	country_name VARCHAR(50),
	country_nicename VARCHAR(50),
	country_numcode INT,
	country_phonecode INT
);

CREATE TABLE IF NOT EXISTS Dim_Reference_Domain (
	reference_key INT PRIMARY KEY,
	reference_domain VARCHAR(255),
	is_self_reference BOOLEAN
);

DROP TABLE IF EXISTS Dim_Operating_System;
CREATE TABLE IF NOT EXISTS Dim_Operating_System (
	os_key INT PRIMARY KEY,
	os_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Dim_Browser (
	browser_key INT PRIMARY KEY,
	browser_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Fact_View (
	key VARCHAR(255) PRIMARY KEY, 
	product_key INT NOT NULL,
	location_key INT NOT NULL,
	date_key INT NOT NULL,
	reference_key INT NOT NULL,
	os_key INT NOT NULL,
	browser_key INT NOT NULL,
	store_id INT,
	total_view INT
);