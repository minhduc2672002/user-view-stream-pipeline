--Top 10 product có view cao nhất
SELECT 
	dp.product_name,
	full_date,
	SUM(total_view) AS total_view
FROM fact_view f
LEFT JOIN dim_date dd ON f.date_key = dd.datetime_key
LEFT JOIN dim_product dp ON f.product_key =  dp.product_key
WHERE f.product_key <> -1 
		AND dp.product_key IS NOT NULL
		AND FLOOR(f.date_key / 100) = (SELECT MAX(FLOOR(date_key / 100)) FROM fact_view)
GROUP BY 1,2
ORDER BY full_date DESC,total_view DESC


-- Top 10 quốc gia có lượt view cao nhất trong ngày hiện tại (quốc gia được lấy dựa vào `domain`)
SELECT 
	dl.country_name,
	full_date,
	SUM(total_view) AS total_view
FROM fact_view f
LEFT JOIN dim_date dd ON f.date_key = dd.datetime_key
LEFT JOIN dim_location dl ON f.location_key =  dl.location_key
WHERE  FLOOR(f.date_key / 100) = (SELECT MAX(FLOOR(date_key / 100)) FROM fact_view)
		AND dl.country_name IS NOT NULL
GROUP BY 1,2
ORDER BY full_date DESC,total_view DESC


-- Top 5 `referrer_url` có lượt view cao nhất trong ngày hiện tại

SELECT 
	drd.reference_domain,
	full_date,
	SUM(total_view) AS total_view
FROM fact_view f
LEFT JOIN dim_date dd ON f.date_key = dd.datetime_key
LEFT JOIN dim_reference_domain drd ON f.reference_key =  drd.reference_key AND drd.is_self_reference = False
WHERE  FLOOR(f.date_key / 100) = (SELECT MAX(FLOOR(date_key / 100)) FROM fact_view)
		AND drd.reference_domain IS NOT NULL
		AND drd.reference_domain <> 'Undefined'
GROUP BY 1,2
ORDER BY full_date DESC,total_view DESC

-- Với 1 quốc gia bất kỳ, lấy ra danh sách các `store_id` và lượt view tương ứng, sắp xếp theo lượt view giảm dần
SELECT 
	dl.country_name,
	f.store_id,
	SUM(total_view) AS total_view
FROM fact_view f
LEFT JOIN dim_location dl ON f.location_key =  dl.location_key
WHERE dl.location_key IS NOT NULL
		AND store_id IS NOT NULL
GROUP BY 1,2
ORDER BY total_view DESC,country_name DESC

-- Dữ liệu view phân bổ theo giờ của một `product_id` bất kỳ trong ngày gần nhất
-- Tạo function
CREATE OR REPLACE FUNCTION get_product_view_stats(
    product_key_input INT,-- Tham số cho product_key
)
RETURNS TABLE (
    product_name character varying(50),        -- Tên sản phẩm
    hour INT,                 -- Giờ trong ngày
    full_date DATE,           -- Ngày đầy đủ
    total_view BIGINT            -- Tổng số lượt xem
) AS
$$
BEGIN
    RETURN QUERY
    SELECT 
		dp.product_name,
		dd.hour,
		dd.full_date,
		SUM(f.total_view) AS total_view
	FROM fact_view f
	LEFT JOIN dim_date dd ON f.date_key = dd.datetime_key
	LEFT JOIN dim_product dp ON f.product_key =  dp.product_key
	WHERE f.product_key <> -1 
			AND dp.product_key IS NOT NULL
			AND FLOOR(f.date_key / 100) = (SELECT MAX(FLOOR(date_key / 100)) FROM fact_view)
			AND f.product_key = product_key_input
	GROUP BY 1,2,3
	ORDER BY dp.product_name,dd.hour,dd.full_date;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_product_view_stats(104183);

-- Dữ liệu view theo giờ của từng `browser`, `os`
-- Theo browser
WITH view_by_date AS (
SELECT 
	f.browser_key,
	dd.hour,
	SUM(total_view) AS total_view
FROM fact_view f
LEFT JOIN dim_date dd ON f.date_key = dd.datetime_key
GROUP BY 1,2
)

SELECT 
	br.browser_name,
	vd.hour,
	vd.total_view
FROM view_by_date vd
LEFT JOIN dim_browser br ON vd.browser_key =  br.browser_key
ORDER BY vd.hour,vd.total_view DESC

--Theo OS
WITH view_by_date AS (
SELECT 
	f.os_key,
	dd.hour,
	SUM(total_view) AS total_view
FROM fact_view f
LEFT JOIN dim_date dd ON f.date_key = dd.datetime_key
GROUP BY 1,2
)

SELECT 
	os.os_name,
	vd.hour,
	vd.total_view
FROM view_by_date vd
LEFT JOIN dim_operating_system os ON vd.os_key =  os.os_key
ORDER BY vd.hour,vd.total_view DESC
