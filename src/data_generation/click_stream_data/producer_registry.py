import pandas as pd
from faker import Faker
from click_stream_provider import ClickStreamProvider
from confluent_kafka import Producer , AdminClient , NewTopic
import json

BOOTSTRAP = "localhost:29092"
USER_TOPIC = "user-events"
ORDER_TOPIC = "order-events"

# customers = pd.read_csv("src/data_generation/generated_data/customers.csv", usecols=["customer_id"])
# products  = pd.read_csv("src/data_generation/generated_data/products.csv",  usecols=["product_id"])

# #  Configure & register the provider
# fake = Faker()
# ClickStreamProvider.configure(customers["customer_id"], products["product_id"])
# fake.add_provider(ClickStreamProvider)


# producer = Producer({"bootstrap.servers": "localhost:29092"})

# def delivery_report(err, msg):
#     if err is not None:
#         print(f"❌ Delivery failed: {err}")
#     else:
#         print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] "
#               f"offset {msg.offset()}")

# # 4⃣  Generate ONE event and send it
# event = fake.click_event()
# print(event)
# producer.produce(
#     topic="clickstream-events",
#     key=str(event["user_id"]),
#     value=json.dumps(event).encode(),
#     callback=delivery_report
# )

# # 5⃣  Block until all messages are sent
# producer.flush()


def ensure_topics(admin: AdminClient, topics: list[str], partitions=3, rf=1):
    new = [NewTopic(t, num_partitions=partitions, replication_factor=rf) for t in topics]
    futures = admin.create_topics(new)
    for t, f in futures.items():
        try:
            f.result()
            print(f"✔ created topic {t}")
        except Exception as e:
            if "TopicExists" in str(e) or "already exists" in str(e):
                pass  # ok
            else:
                raise

def topic_for_event(event: dict) -> str:
    return ORDER_TOPIC if event["event_type"] == "purchase" else USER_TOPIC

def delivery_cb(err, msg):
    if err:
        print(f"❌ delivery failed: {err}")
    else:
        print(f"→ {msg.topic()} key={msg.key().decode()} offset={msg.offset()}")

def main(n=50):
    # Demo dimensions (swap with your CSVs)
    customers = pd.DataFrame({"customer_id": range(1, 101)})
    products  = pd.DataFrame({"product_id":  range(1, 51)})

    fake = Faker()
    ClickStreamProvider.configure(customers["customer_id"], products["product_id"])
    fake.add_provider(ClickStreamProvider)

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    ensure_topics(admin, [USER_TOPIC, ORDER_TOPIC])

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5
    })

    for _ in range(n):
        event = fake.click_event()
        topic = topic_for_event(event)
        producer.produce(
            topic=topic,
            key=str(event["user_id"]),
            value=json.dumps(event).encode(),
            headers=[("event_type", event["event_type"].encode()),
                     ("schema", b"v1")],
            callback=delivery_cb
        )

    producer.flush()

if __name__ == "__main__":
    main(n=100)