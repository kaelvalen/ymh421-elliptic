# YMH421 — Spark + Jupyter çalışma ortamı
# Python 3.12 + OpenJDK 17 + PySpark 3.5.8 + GraphFrames 0.12.3 + Kafka connector
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java   # amd64 ve arm64 için aynı yol

ENV JAVA_HOME=/opt/java \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# GraphFrames ve Kafka JVM bağımlılıklarını imaj build edilirken indir:
# sunum günü internet bağlantısına gerek kalmasın.
COPY common/ /tmp/warm/common/
RUN cd /tmp/warm && python -c "from common.spark_utils import get_spark; get_spark('warmup').stop()" \
 && rm -rf /tmp/warm

WORKDIR /work
EXPOSE 8888 4040 4041 4042 4043 4044 4045
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.token=", "--ServerApp.password=", "--ServerApp.root_dir=/work"]
