# Web Log Analizi 

## Proje Özeti

Bu proje, web sunucusu loglarını (`access.log` ve `error.log`) analiz ederek anomali tespiti ve hata özetleme için bir veri mühendisliği projesidir. Apache Spark veri işleme, Hadoop HDFS depolama, Apache Hive sorgulama ve PostgreSQL Hive Metastore olarak kullanılır. Docker tabanlı bir ortamda çalışır ve DBeaver ile SQL tabanlı analiz yapılır. Temel analizler, belirli IP’ler için anomali tespiti, sık görülen hataların belirlenmesi ve saatlik hata dağılımlarını içerir. Projede görselleştirme bulunmuyor.

## Mimari

- **Veri Kaynakları**: Web sunucusu logları (`access.log`, `error.log`), `spark/logs/` dizininde saklanır.
- **İşleme**: Apache Spark logları okur ve ayrıştırır, ardından partition’lı parquet dosyaları olarak HDFS’e yazar.
- **Depolama**: HDFS (`hdfs://cluster-master:9000/logs/`), logları `log_date` partition’larıyla saklar (ör. `date=2025-04-11`).
- **Sorgulama**: Hive external tabloları (`ext_access_log`, `ext_errors_log`), HDFS verilerine bağlanır ve DBeaver ile sorgulanır.
- **Metastore**: PostgreSQL, Hive metadata’sını saklar.
- **Ortam**: Hadoop, Spark, Hive ve PostgreSQL için Docker konteynerleri.

## Ön Koşullar

- Docker ve Docker Compose

## Kurulum

1. **Depoyu Klonlayın**:

   ```bash
   git clone https://github.com/emrefkrlr/web-log-analysis.git
   cd web-log-analysis
   ```

2. **Docker Konteynerlerini Başlatın**:

   ```bash
   docker-compose up -d
   ```

3. **Logları Hazırlayın**:

   - https://github.com/emrefkrlr/log_generator.git reposundan logları oluştur.
   - `access.log` ve `error.log` dosyalarını `spark/logs/` dizinine yerleştirin.

## Kullanım

1. **Logları Ayrıştırın ve HDFS’e Yazın**:

   - `spark_client` içinde Jupyter Notebook’u açın: http://localhost:8888`

   - `read_logs.ipynb`’yi çalıştırarak logları ayrıştırın ve `hdfs://cluster-master:9000/logs/`’a yazın.

2. **Hive Tablolarını Oluşturun**:
   
   - Hive' ı başlat
````
# cluster-mastera gir
docker exec -it cluster-master bash

# Hive serverı çalıştır
root@cluster-master:/dataops# $HADOOP_HOME/start-hive.sh

Services starting up. Waiting for 60 seconds...
Hive Metastore and HiveServer2 services have been started successfully.
````

   - DBeaver ile Hive’a bağlanın (`jdbc:hive2://localhost:10000/default`).

      Linkteki adımları takip ederek Hive bağlantısı yapabilirsin.

      https://dbeaver.com/docs/dbeaver/Apache-Hive/

   - Tabloları oluşturmak ve analizler için Hive dizini içinde bulunan sql leri kullanabilirsin

## Proje Yapısı

```
web-log-analysis/
├── hive
│   ├── access_log.sql                       # Hive ext_access_log tablosu ve anomali testi
│   ├── errors_log.sql                       # Hive ext_errors_log tablosu ve diğer analizler
├── postgresql                               # Postgresql, HDFS, Hive kurulumu için gerekli
│   ├── core-site.xml
│   ├── hive-site.xml
│   ├── init.sql
├── spark/
│   ├── logs/                                # Giriş log dosyalarını buraya ekle   
│   ├── read_logs_and_write_hdfs.ipynb       # Spark log ayrıştırma ve HDFS’e yazma    
├── docker-compose.yml                       # Docker konfigürasyonu
└── README.md                                # Proje dokümantasyonu
```

## Teknolojiler

- Apache Spark
- Apache Hadoop (HDFS)
- Apache Hive
- PostgreSQL
- Docker
- DBeaver
