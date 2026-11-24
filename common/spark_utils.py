"""Ortak SparkSession kurulumu ve proje yolları.

Tüm notebook'lar buradaki get_spark() ile aynı konfigürasyonu kullanır;
sürüm sabitlemesi tek bir yerde durur.
"""
from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession

SPARK_VERSION = "3.5.8"
GRAPHFRAMES_VERSION = "0.12.3"
PACKAGES = [
    f"io.graphframes:graphframes-spark3_2.12:{GRAPHFRAMES_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
]


def project_root() -> Path:
    here = Path.cwd().resolve()
    for cand in [here, *here.parents]:
        if (cand / "common" / "spark_utils.py").exists():
            return cand
    raise RuntimeError("Proje kökü bulunamadı (common/spark_utils.py yok).")


ROOT = project_root()
RAW = Path(os.environ.get("ELLIPTIC_RAW", ROOT / "data" / "raw"))
PROC = ROOT / "data" / "processed"
MODELS = ROOT / "models"
CHECKPOINTS = ROOT / "checkpoints"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "btc-tx")

# Zamansal bölme: 1-34 eğitim, 35-49 test (Weber et al., 2019 ile aynı).
TRAIN_MAX_STEP = 34


def get_spark(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.jars.packages", ",".join(PACKAGES))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(str(CHECKPOINTS / "graph"))
    return spark


def stage_metrics(spark: SparkSession, last_n: int = 5) -> list[dict]:
    """Spark UI REST API'sinden son tamamlanan stage'lerin shuffle metriklerini okur."""
    sc = spark.sparkContext
    url = f"http://localhost:{sc.uiWebUrl.rsplit(':', 1)[1]}/api/v1/applications/{sc.applicationId}/stages?status=complete"
    with urllib.request.urlopen(url) as r:
        stages = json.load(r)
    stages.sort(key=lambda s: s["stageId"], reverse=True)
    return [
        {
            "stageId": s["stageId"],
            "name": s["name"][:60],
            "shuffleWriteKB": round(s["shuffleWriteBytes"] / 1e3, 1),
            "shuffleReadKB": round(s["shuffleReadBytes"] / 1e3, 1),
            "durationMs": s.get("executorRunTime"),
        }
        for s in stages[:last_n]
    ]
