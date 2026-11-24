# YMH421 Büyük Veri Analitiği — Apache Spark ile Illicit Bitcoin İşlem Analizi

Beş aşamanın tamamı tek bir veri seti üzerinde yapılıyor: **Elliptic++**. Bu veri seti, Bitcoin işlem grafını (203.769 işlem, 234.355 BTC akışı, 49 zaman adımı) ve cüzdan grafını (822.942 adres, 2.87M adres-adres kenarı) içeriyor; işlemler illicit, licit veya unknown olarak etiketlenmiş.

| # | Aşama | Notebook | Spark bileşeni | Ana bulgu |
|---|---|---|---|---|
| 1 | Ön işleme + MapReduce | `01_preprocessing_mapreduce.ipynb` | DataFrame, **RDD** (`flatMap`, `reduceByKey`) | Cüzdan tablosunun %27'si kopya satır; 965 işlemde 17 özellik birlikte eksik; `reduceByKey` ile `groupByKey` arasındaki shuffle farkı anahtar kardinalitesine bağlı |
| 2 | Makine öğrenmesi | `02_mllib_classification.ipynb` | **MLlib** (Pipeline, RF, LR, Imputer) | Zamansal bölmede illicit F1 = 0.83; 43. adımdan sonra 0.03'e çöküyor (concept drift); rastgele bölme skoru +0.11 abartıyor |
| 3 | Gerçek zamanlı akış | `03_structured_streaming.ipynb` + `streaming/producer.py` | **Structured Streaming** + **Kafka** | Canlı skorlama, olay zamanı pencereleri + watermark, exactly-once file sink, checkpoint'ten devam |
| 4 | SQL analizi | `04_spark_sql.ipynb` | **Spark SQL** (7 sorgu, JOIN, CTE, window) | İllicit→illicit homofilisi taban oranın 5 katı; cüzdan etiketleri işlem etiketlerinden deterministik türetilmiş (sızıntı riski) |
| 5 | Graf analizi | `05_graphframes.ipynb` | **GraphFrames** (degree, CC, PageRank, motif) | illicit→illicit zincirinde üçüncü halkanın illicit olma olasılığı taban oranın 24.5 katı; adres grafı scale-free |

```
                 ┌──────────────────────── data/raw (CSV, 1.6 GB) ────────────────────────┐
                 ▼                                                                        │
  01  ön işleme + MapReduce ──▶ data/processed (Parquet, ~500 MB) ──┬──▶ 02 MLlib ──▶ models/rf_illicit
                                                                    ├──▶ 04 Spark SQL           │
                                                                    ├──▶ 05 GraphFrames         │
                                                                    │                           ▼
  streaming/producer.py ──JSON──▶ Kafka "btc-tx" ──▶ 03 Structured Streaming (feature store join + model)
                                                                    └──▶ data/stream_out/alerts (Parquet)
```

---

## 1. Kurulum (önerilen: Docker)

Gereksinimler: Docker Desktop (veya Docker Engine + Compose v2) ve en az **8 GB** RAM (Docker Desktop'ta *Settings → Resources* altından ayarlanır).

```bash
# 1) Veriyi indir (≈1.6 GB, SHA-256 doğrulamalı)
./scripts/download_data.sh

# 2) Ortamı ayağa kaldır: Kafka (KRaft) + JupyterLab/Spark
docker compose up -d --build

# 3) Tarayıcıda aç
#    JupyterLab : http://localhost:8888
#    Spark UI   : http://localhost:4040  (her açık notebook bir sonraki portu alır: 4041, 4042 …)
```

İmaj build edilirken GraphFrames ve Kafka JVM bağımlılıkları da indirilir. Bu yüzden **sunum günü internet bağlantısı gerekmez**.

Bellek ayarı: `SPARK_DRIVER_MEMORY=4g docker compose up -d` (varsayılan 6g).

### Docker'sız kurulum

Python 3.11–3.13 ve Java 17 (21 ile de test edildi) gerekli.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
./scripts/download_data.sh
# Kafka (KRaft, ZooKeeper'sız): https://kafka.apache.org/downloads
bin/kafka-storage.sh format --standalone -t "$(bin/kafka-storage.sh random-uuid)" -c config/server.properties
bin/kafka-server-start.sh config/server.properties
# ayrı terminalde:
export KAFKA_BOOTSTRAP=localhost:9092
jupyter lab
```

## 2. Çalıştırma sırası

Notebook'lar **sırayla** çalıştırılmalı, çünkü her biri bir öncekinin çıktısını kullanıyor:

| Sıra | Notebook | Ürettiği | Süre* |
|---|---|---|---|
| 1 | `01_preprocessing_mapreduce` | `data/processed/*` | ~5 dk |
| 2 | `02_mllib_classification` | `models/rf_illicit` | ~6 dk |
| 3 | `03_structured_streaming` | `data/stream_out/alerts` | demo süresi |
| 4 | `04_spark_sql` | — | ~3 dk |
| 5 | `05_graphframes` | — | ~5 dk |

\* 2 çekirdek / 8 GB RAM ile ölçüldü; daha güçlü bir dizüstünde daha kısa sürer. 4 ve 5, 2'den sonra herhangi bir sırada çalıştırılabilir.

Her notebook kendi `SparkSession`'ını açıp en sonda kapatır. Bellek sıkışmasın diye **aynı anda tek notebook** çalıştırın.

## 3. Canlı streaming demosu (sunum)

İki pencere açın:

**Pencere 1: JupyterLab.** `03_structured_streaming.ipynb` dosyasını §4'teki "Canlı pano" hücresine kadar çalıştırın (topic sıfırlanır, üç sorgu başlar).

**Pencere 2: terminal.** Producer'ı başlatın:
```bash
docker compose exec jupyter python streaming/producer.py --from-step 40 --rate 300 --step-pause 3
```

Ardından pano hücresini çalıştırın. Pano 5 saniyede bir yenilenir ve şunları gösterir:
- her sorgunun batch numarası, girdi/işlenen satır hızı, watermark'ı ve watermark yüzünden atılan kayıt sayısı,
- son alarmlar (Kafka partition ve offset bilgisiyle),
- 30 saniyelik olay zamanı pencereleri,
- canlı model performansı, toplam ve zaman adımı bazında. `--from-step 40` ile başlatıldığında 43. adımdaki çöküş birkaç dakika içinde ekranda görünür.

Producer parametreleri: `--rate` (mesaj/sn), `--from-step` / `--to-step`, `--late-ratio` (geç kayıt oranı, varsayılan 0.02), `--late-seconds` (varsayılan 150 sn, watermark 60 sn), `--limit`.

Host'tan çalıştırmak için `--bootstrap localhost:9094` kullanın. Producer'ı notebook'un kendisinin başlatması için Jupyter'i `DEMO_EMBEDDED_PRODUCER=1` ortam değişkeniyle açın.

Kafka'yı terminalden izlemek için:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic btc-tx --max-messages 5
```

## 4. Sürümler

| Bileşen | Sürüm | Not |
|---|---|---|
| Apache Spark / PySpark | 3.5.8 | 3.5 hattı; Scala 2.12 |
| GraphFrames | 0.12.3 | JVM: `io.graphframes:graphframes-spark3_2.12:0.12.3`, Python: `graphframes-py==0.12.3` |
| Spark-Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.8` | Spark sürümüyle **birebir** aynı olmalı |
| Apache Kafka | 4.3.2 (KRaft) | `apache/kafka` imajı |
| Python | 3.12 (Docker), 3.13 ile de test edildi | |
| Java | OpenJDK 17 (Docker), 21 ile de test edildi | |

JVM paket koordinatları tek bir yerde tanımlı: `common/spark_utils.py → PACKAGES`.

## 5. Proje yapısı

```
├── notebooks/                    5 aşama (çıktılarıyla birlikte)
├── streaming/producer.py         Kafka producer (test adımlarını gerçek zamanlıymış gibi oynatır)
├── common/spark_utils.py         ortak SparkSession, yollar, sürüm sabitleri, shuffle metrikleri
├── scripts/download_data.sh      veri indirme + SHA-256 doğrulama
├── Dockerfile, docker-compose.yml, requirements.txt
├── data/raw/                     ham CSV'ler (git'e eklenmez)
├── data/processed/               Aşama 1 çıktısı (Parquet)
├── data/stream_out/              Aşama 3 alarm çıktısı (Parquet)
├── models/                       Aşama 2 modeli + metadata
└── checkpoints/                  streaming checkpoint'leri, graf checkpoint'leri
```

## 6. Sorun giderme

| Belirti | Neden / çözüm |
|---|---|
| `java.lang.ClassNotFoundException: org.graphframes...` veya `py4j ... graphframes` | JVM paketi ile Spark sürümü uyumsuz. `PACKAGES` içindeki koordinatların `pyspark` sürümüyle eşleştiğini kontrol edin. |
| `Failed to find data source: kafka` | `spark-sql-kafka` paketi yüklenmemiş ya da sürümü farklı. İlk çalıştırmada Maven erişimi gerekir; Docker imajında hazır gelir. |
| Streaming hücresi veri almıyor | Producer çalışıyor mu? `KAFKA_BOOTSTRAP` doğru mu? (container içinde `kafka:9092`, host'ta `localhost:9094`) |
| `This query does not support recovering from checkpoint` | Notebook 03'ün ilk hücresi checkpoint'leri temizler; hücreleri baştan çalıştırın. |
| `OutOfMemoryError` / kernel ölüyor | `SPARK_DRIVER_MEMORY` değerini artırın veya Docker'a ayrılan belleği yükseltin; aynı anda tek notebook çalıştırın. |
| İndirmede SHA uyuşmazlığı | GitHub LFS bant genişliği kotası dolmuş olabilir. Veriyi [repo README'sindeki](https://github.com/git-disl/EllipticPlusPlus) Google Drive bağlantısından indirip `data/raw/` altına (alt klasör olmadan) koyun. |

## 7. Yönerge ile eşleşme

| Yönerge maddesi | Nerede |
|---|---|
| Veri yükleme, eksik/hatalı veri, tekrar, tip, filtre, dönüşüm, ölçekleme ve etkilerin gösterimi | 01 §1–2.5 (önce/sonra tabloları) |
| RDD ile `map` / `flatMap` / `reduceByKey`, Map ve Reduce aşamalarının açıklanması | 01 MR-1…MR-4 |
| MLlib ile model, özellik hazırlama, değerlendirme metrikleri, yorum | 02 (sklearn kullanılmadı) |
| Producer → message broker → Structured Streaming; filtreleme, dönüşüm, toplulaştırma | 03 + `streaming/producer.py` |
| Temp view üzerinde en az 5 anlamlı sorgu (SELECT/WHERE/GROUP BY/ORDER BY/JOIN/fonksiyonlar) | 04 (7 sorgu + `EXPLAIN`) |
| GraphFrames ile düğüm/kenar tanımı ve graf algoritmaları | 05 (degree, connected components, PageRank, motif) |
| Kütüphane, bağımlılık ve sürümlerin belirtilmesi | bu dosya §4, `requirements.txt`, `Dockerfile` |

## Kaynaklar

- Elmougy, Y. & Liu, L. (2023). *Demystifying Fraudulent Transactions and Illicit Nodes in the Bitcoin Network for Financial Forensics.* KDD '23. Veri: https://github.com/git-disl/EllipticPlusPlus
- Weber, M. et al. (2019). *Anti-Money Laundering in Bitcoin: Experimenting with Graph Convolutional Networks for Financial Forensics.* KDD '19 Workshop on Anomaly Detection in Finance. arXiv:1908.02591
- Apache Spark 3.5 dokümantasyonu: Structured Streaming Programming Guide, Structured Streaming + Kafka Integration Guide, MLlib Guide, Spark SQL Guide
- GraphFrames dokümantasyonu: https://graphframes.io
- Apache Kafka dokümantasyonu (KRaft modu)
