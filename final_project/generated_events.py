import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import atexit
import subprocess
import os

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# HDFS upload function
def upload_to_hdfs():
    local_file = "events/events_log.json"
    hdfs_path = "hdfs://localhost:9000/user/harvijaysingh/events_log/events_log.json"
    try:
        # Ensure HDFS directory exists
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/user/harvijaysingh/events_log"], check=True)
        # Upload file to HDFS
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_path], check=True)
        print(f"Successfully uploaded {local_file} to {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading to HDFS: {e}")
    except Exception as e:
        print(f"Unexpected error during HDFS upload: {e}")

# Register HDFS upload to run on script termination
atexit.register(upload_to_hdfs)

devices = ["Mobile", "Desktop", "Tablet"]
event_types = ["view_product", "add_to_cart", "purchase", "search", "login", "logout"]
categories = ["Electronics", "Fashion", "Home", "Books", "Beauty", "Toys", "Grocery", "Furniture", "Sports and Fitness"]
product_types = {
    "Electronics": ["Smartphone", "Laptop", "Camera"],
    "Fashion": ["Shoes", "Shirt", "Dress"]
}
brands_by_category = {
    "Electronics": ["Apple", "Samsung", "Sony"],
    "Fashion": ["Nike", "Zara", "Puma"]
}

while True:
    try:
        event_type = random.choice(event_types)
        event = {
            "event_id": f"U{random.randint(1000, 9999)}",
            "event_type": event_type,
            "timestamp": time.time()
        }
        if event_type not in ["login", "logout"]:
            num_products = random.randint(1, 3)
            products = []
            for _ in range(num_products):
                category = random.choice(categories)
                product = {
                    "product_id": f"P{random.randint(100, 999)}",
                    "product_category": category,
                    "price": f"${random.randint(500, 9999)}"
                }
                if category in product_types:
                    product["product_type"] = random.choice(product_types[category])
                if category in brands_by_category:
                    product["brand"] = random.choice(brands_by_category[category])
                products.append(product)
            event["products"] = products
        
        json_event = json.dumps(event)
        print(json_event)
        
        # Write to file
        with open("events_log.json", "a", encoding="utf-8") as file:
            file.write(json_event + "\n")
            file.flush()
        
        # Send to Kafka topic
        try:
            producer.send('test-events', event)
            producer.flush()
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
        
        time.sleep(random.uniform(0.5, 5))   
     
    except KeyboardInterrupt:
        print("Producer stopped by user")
        producer.close()
        break