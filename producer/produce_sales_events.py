#!/usr/bin/env python3
"""
produce_sales_events.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mock producer sinh event bán hàng giả lập vào Kafka topic.

Mỗi event là một đơn hàng với:
- event_time   : thời gian hiện tại
- order_id     : ID tăng dần (unique trong demo)
- province     : tỉnh/thành ngẫu nhiên
- product      : sản phẩm ngẫu nhiên
- amount       : doanh thu ngẫu nhiên (50k - 500k VND)
- payment_method: phương thức thanh toán ngẫu nhiên

Lưu ý:
- DUPLICATE KEY table trong StarRocks → mỗi order_id phải unique
  để tránh double count metric. Demo này đảm bảo điều đó.
- Nếu cần test upsert/CDC → đổi table sang PRIMARY KEY và gửi
  events có cùng order_id với khác amount.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import os
import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "localhost:9094")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC", "sales_events")
EVENTS_PER_SECOND  = float(os.getenv("EVENTS_PER_SECOND", "2"))
SLEEP_INTERVAL     = 1.0 / EVENTS_PER_SECOND

# ─── Data pools ───────────────────────────────────────────────────────────────
PROVINCES = [
    "Hanoi", "Ho Chi Minh", "Da Nang", "Can Tho",
    "Hai Phong", "Bien Hoa", "Hue", "Nha Trang",
    "Vung Tau", "Quy Nhon", "Vinh", "Thai Nguyen"
]

PRODUCTS = [
    "Data Package 5GB",
    "Data Package 10GB",
    "Data Package Unlimited",
    "Voice Package 100p",
    "Voice Package 300p",
    "Roaming Package SEA",
    "Premium Bundle",
    "Device Installment",
    "Service Fee Monthly",
    "International Call Pack"
]

PAYMENT_METHODS = ["CARD", "CASH", "BANKING", "WALLET"]

AMOUNT_RANGES = {
    "Data Package 5GB":         (50_000,  99_000),
    "Data Package 10GB":        (100_000, 149_000),
    "Data Package Unlimited":   (200_000, 299_000),
    "Voice Package 100p":       (50_000,  79_000),
    "Voice Package 300p":       (120_000, 160_000),
    "Roaming Package SEA":      (200_000, 350_000),
    "Premium Bundle":           (300_000, 499_000),
    "Device Installment":       (200_000, 500_000),
    "Service Fee Monthly":      (30_000,  60_000),
    "International Call Pack":  (150_000, 250_000),
}


# ─── Producer setup ───────────────────────────────────────────────────────────
def create_producer(broker: str, max_retries: int = 30) -> KafkaProducer:
    """Kết nối Kafka với retry logic (Kafka có thể chưa sẵn sàng)."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",             # Wait for all replicas (demo có 1 replica)
                retries=3,
                linger_ms=100,          # Batch events trong 100ms để tăng throughput
                batch_size=16384,
            )
            print(f"✅ Kafka producer connected to {broker}")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Attempt {attempt}/{max_retries}: Kafka not ready, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            time.sleep(5)

    print("❌ Could not connect to Kafka after max retries. Exiting.")
    sys.exit(1)


def make_event(order_id: int) -> dict:
    """Sinh một event bán hàng ngẫu nhiên."""
    product = random.choice(PRODUCTS)
    lo, hi  = AMOUNT_RANGES.get(product, (50_000, 500_000))
    amount  = round(random.uniform(lo, hi), 2)

    return {
        "event_time":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "order_id":       order_id,
        "province":       random.choice(PROVINCES),
        "product":        product,
        "amount":         amount,
        "payment_method": random.choice(PAYMENT_METHODS),
    }


def on_send_success(metadata):
    pass  # Quiet mode, chỉ log per-batch ở dưới


def on_send_error(exc):
    print(f"❌ Send error: {exc}", file=sys.stderr)


# ─── Main loop ────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("🚀 Kafka Producer: Sales Events")
    print(f"   Broker : {KAFKA_BROKER}")
    print(f"   Topic  : {KAFKA_TOPIC}")
    print(f"   Rate   : {EVENTS_PER_SECOND} events/second")
    print("=" * 60)

    producer   = create_producer(KAFKA_BROKER)
    order_id   = random.randint(100_000, 199_999)  # Start từ random ID
    sent_count = 0
    start_time = time.time()
    log_every  = max(1, int(EVENTS_PER_SECOND * 10))  # Log mỗi 10 giây

    try:
        while True:
            order_id  += 1
            event      = make_event(order_id)
            sent_count += 1

            producer.send(KAFKA_TOPIC, value=event).add_errback(on_send_error)

            # Log progress mỗi N events
            if sent_count % log_every == 0:
                elapsed   = time.time() - start_time
                rate_real = sent_count / elapsed
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"Sent {sent_count:,} events | "
                    f"Rate: {rate_real:.1f} ev/s | "
                    f"Last: order_id={event['order_id']}, "
                    f"province={event['province']}, "
                    f"amount={event['amount']:,.0f}"
                )

            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        print("\n⚠️  Producer stopped by user")
    finally:
        producer.flush()
        producer.close()
        print(f"✅ Total sent: {sent_count:,} events")


if __name__ == "__main__":
    main()
