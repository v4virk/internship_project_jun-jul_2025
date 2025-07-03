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
    local_file = "events_log.json"
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
categories = ["Electronics", "Fashion", "Home", "Books", "Beauty", "Toys", "Grocery", "Furniture", "Sports and Fitness","Automotive", "Pet Supplies"]
product_types = {
    "Electronics": ["Smartphone", "Laptop", "Tablet", "Headphones", "Smartwatch", "Television"],
    "Fashion": ["Shoes", "Shirt", "Dress", "Jeans", "Jacket", "Sunglasses"],
    "Home": ["Bedsheet", "Lamp", "Curtains", "Mattress", "Cookware", "Carpet"],
    "Books": ["Fiction", "Non-Fiction", "Comics", "Textbook", "Biography"],
    "Beauty": ["Lipstick", "Foundation", "Eyeliner", "Perfume", "Skincare Cream"],
    "Toys": ["Action Figure", "Puzzle", "Doll", "Board Game", "Lego Set"],
    "Grocery": ["Rice", "Pasta", "Milk", "Snacks", "Juice", "Spices"],
    "Furniture": ["Sofa", "Dining Table", "Chair", "Bookshelf", "Bed"],
    "Sports and Fitness": ["Treadmill", "Dumbbells", "Yoga Mat", "Cycle", "Tennis Racket"],
    "Automotive": ["Car Battery", "Tyre", "Car Vacuum", "Air Freshener"],
    "Pet Supplies": ["Dog Food", "Cat Litter", "Pet Shampoo", "Pet Toy"]
}
brands_by_category = {
   "Electronics": ["Apple", "Samsung", "Sony", "Dell", "Lenovo", "LG"],
    "Fashion": ["Nike", "Zara", "Puma", "Adidas", "H&M", "Levi's"],
    "Home": ["Philips", "Prestige", "Usha", "Cello", "Milton"],
    "Books": ["Penguin", "HarperCollins", "Scholastic", "Bloomsbury"],
    "Beauty": ["L'Oreal", "Maybelline", "Lakme", "Nivea", "The Body Shop"],
    "Toys": ["Lego", "Barbie", "Hot Wheels", "Funskool", "Fisher-Price"],
    "Grocery": ["Nestle", "Amul", "Patanjali", "Tata", "Aashirvaad"],
    "Furniture": ["Ikea", "Godrej", "Urban Ladder", "Pepperfry"],
    "Sports and Fitness": ["Decathlon", "Nike", "Adidas", "Reebok"],
    "Automotive": ["Bosch", "Michelin", "Goodyear", "3M"],
    "Pet Supplies": ["Pedigree", "Whiskas", "Drools", "Himalaya"]
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
        
        time.sleep(1)
    
    except KeyboardInterrupt:
        print("Producer stopped by user")
        producer.close()
        break
