-- External table oluşturma
create external table if not exists default.ext_errors_log
(
	log_timestamp timestamp,
	level string,
	client_ip string,
	message string
)
partitioned by (log_date date)
stored as PARQUET
location 'hdfs://cluster-master:9000/logs/errors';

-- HDFS deki verileri tabloya aktarma
MSCK REPAIR TABLE ext_errors_log;

-- En Sık Alınan Hatalar
SELECT message, COUNT(*) as count
FROM default.ext_errors_log
GROUP BY message
ORDER BY count DESC LIMIT 10;

-- Hataların gün içinde saatlik sayıları
SELECT 
    log_date,
    HOUR(log_timestamp) as hour,
    COUNT(*) as error_count
FROM default.ext_errors_log
GROUP BY log_date, HOUR(log_timestamp)
ORDER BY log_date, error_count DESC;