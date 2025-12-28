from confluent_kafka import Producer
import json
import time
import csv

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

with open("../data/processed/cars_1990.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        event = {
            "year": int(row["Year"]),
            "make": row["Fabricant"],
            "model": row["Model"],
            "trim": row["Trim"],
            "body": row["Body"],
            "transmission": row["Transmission"],
            "state": row["State"],
            "condition": float(row["Condition"]) if row["Condition"] else None,
            "odometer": float(row["Odometer"]) if row["Odometer"] else None,
            "color": row["Color"],
            "interior": row["Interior"],
            "seller": row["Seller"],
            "mmr": float(row["MMR"]) if row["MMR"] else None,
            "sellingPrice": float(row["SellingPrice"]) if row["SellingPrice"] else None,
            "saleDate": row["SaleDate"],
        }
        producer.produce(
            "test-topic",
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.1)

producer.flush()
