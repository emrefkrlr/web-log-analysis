-- External table oluşturma
create external table if not exists default.ext_access_log
(
	ip string,
	log_timestamp timestamp,
	log_method string,
	url string,
	protocol string,
	status string,
	log_size string,
	referer string,
	user_agent string
)
partitioned by (log_date date)
stored as PARQUET
location 'hdfs://cluster-master:9000/logs/access';

-- HDFS deki verileri tabloya aktarma
MSCK REPAIR TABLE ext_access_log;


-- Anomali Tespiti

WITH request_counts AS (
    -- Her IP ve tarih için istek sayısını hesapla
    SELECT 
        ip,
        log_date,
        COUNT(*) as request_count
    FROM default.ext_access_log
    GROUP BY ip, log_date
),
stats AS (
    -- Her tarih için ortalama ve standart sapmayı hesapla
    SELECT 
        log_date,
        AVG(request_count) as mean_requests,
        STDDEV(request_count) as stddev_requests
    FROM request_counts
    GROUP BY log_date
),
z_scores AS (
    -- Her IP için Z-skorunu hesapla
    SELECT 
        r.ip,
        r.log_date,
        r.request_count,
        (r.request_count - s.mean_requests) / s.stddev_requests as z_score
    FROM request_counts r
    JOIN stats s ON r.log_date = s.log_date
    WHERE s.stddev_requests > 0  -- Standart sapma 0 ise Z-skoru tanımsız olur
)
-- Z-skoru 2'den büyük olan IP'leri anomali olarak seç
SELECT 
    ip,
    log_date,
    request_count,
    ROUND(z_score, 2) as z_score
FROM z_scores
WHERE ABS(z_score) > 2
ORDER BY log_date, z_score DESC;

-- Anomali Tespiti Son