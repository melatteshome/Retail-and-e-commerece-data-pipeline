import pandas as pd
from faker import Faker
from click_stream_provider import ClickStreamProvider
from confluent_kafka import Producer
import json

customers = pd.read_csv("src/data_generation/generated_data/customers.csv", usecols=["customer_id"])
products  = pd.read_csv("src/data_generation/generated_data/products.csv",  usecols=["product_id"])

#  Configure & register the provider
fake = Faker()
ClickStreamProvider.configure(customers["customer_id"], products["product_id"])
fake.add_provider(ClickStreamProvider)


producer = Producer({"bootstrap.servers": "localhost:29092"})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] "
              f"offset {msg.offset()}")

# 4⃣  Generate ONE event and send it
event = fake.click_event()
print(event)
producer.produce(
    topic="clickstream-events",
    key=str(event["user_id"]),
    value=json.dumps(event).encode(),
    callback=delivery_report
)

# 5⃣  Block until all messages are sent
producer.flush()