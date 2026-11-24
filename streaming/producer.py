"""Elliptic++ işlemlerini Kafka'ya gerçek zamanlıymış gibi yeniden oynatan producer.

Modelin eğitimde görmediği test zaman adımlarını (varsayılan 35-49) sırayla gönderir.
Her mesaj yalnızca işlemin "zincirde görünen" ham alanlarını taşır; tam özellik vektörü
Spark tarafında stream-static join ile feature tablosundan eklenir.

--late-ratio ile olay zamanı geçmişe kaydırılmış "geç gelen" kayıtlar üretilir; bu
kayıtlar Spark'taki watermark davranışını göstermek içindir.

Kullanım:
    python streaming/producer.py --rate 300 --step-pause 5
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.dataset as ds
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ["txId", "time_step", "size", "fees", "num_input_addresses",
          "num_output_addresses", "out_BTC_total"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "btc-tx"))
    p.add_argument("--from-step", type=int, default=35)
    p.add_argument("--to-step", type=int, default=49)
    p.add_argument("--rate", type=float, default=300.0, help="saniyede mesaj")
    p.add_argument("--step-pause", type=float, default=3.0, help="zaman adımları arası bekleme (sn)")
    p.add_argument("--late-ratio", type=float, default=0.02, help="geç gelen kayıt oranı")
    p.add_argument("--late-seconds", type=int, default=150, help="geç kayıtların olay zamanı gecikmesi")
    p.add_argument("--limit", type=int, default=0, help="toplam mesaj sınırı (0 = sınırsız)")
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="elliptic-admin")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"[producer] topic oluşturuldu: {topic} ({partitions} partition)")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def load_events(from_step: int, to_step: int, seed: int) -> list[dict]:
    dataset = ds.dataset(ROOT / "data" / "processed" / "txs", format="parquet")
    table = dataset.to_table(
        columns=FIELDS,
        filter=(ds.field("time_step") >= from_step) & (ds.field("time_step") <= to_step),
    )
    rows = table.to_pylist()
    rng = random.Random(seed)
    by_step: dict[int, list[dict]] = {}
    for r in rows:
        by_step.setdefault(r["time_step"], []).append(r)
    ordered = []
    for step in sorted(by_step):
        bucket = by_step[step]
        rng.shuffle(bucket)          # adım içinde sıra rastgele, adımlar arası sıra korunur
        ordered.extend(bucket)
    return ordered


def main() -> int:
    a = parse_args()
    events = load_events(a.from_step, a.to_step, a.seed)
    if not events:
        print("[producer] gönderilecek kayıt yok; önce 01 notebook'unu çalıştırın.", file=sys.stderr)
        return 1
    ensure_topic(a.bootstrap, a.topic, a.partitions)

    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20,
        acks=1,
    )
    rng = random.Random(a.seed + 1)
    interval = 1.0 / a.rate if a.rate > 0 else 0.0
    sent = late = 0
    current_step = None
    t0 = time.time()
    print(f"[producer] {len(events)} işlem, adım {a.from_step}-{a.to_step}, {a.rate}/sn -> {a.bootstrap}/{a.topic}")
    try:
        for ev in events:
            if ev["time_step"] != current_step:
                if current_step is not None:
                    producer.flush()
                    print(f"[producer] adım {current_step} bitti | toplam {sent} | geç {late}")
                    time.sleep(a.step_pause)
                current_step = ev["time_step"]
            event_time = datetime.now(timezone.utc)
            if rng.random() < a.late_ratio:
                event_time -= timedelta(seconds=a.late_seconds)
                late += 1
            msg = {k: ev[k] for k in FIELDS}
            msg["event_time"] = event_time.isoformat(timespec="milliseconds")
            producer.send(a.topic, key=ev["txId"], value=msg)
            sent += 1
            if a.limit and sent >= a.limit:
                break
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[producer] durduruldu")
    finally:
        producer.flush()
        producer.close()
    print(f"[producer] bitti: {sent} mesaj ({late} geç) {time.time() - t0:.1f} sn")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
