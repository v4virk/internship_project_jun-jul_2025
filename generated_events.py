import time
import json
import random
from datetime import datetime
devices = ["Mobile", "Desktop", "Tablet"]
event_types = ["view_product", "add_to_cart", "purchase", "search", "login", "logout"]
category=["Electronics","Fashion","Home","Books","Beauty","Toys","Grocery","Furniture","Sports and Fitness"]
while True:
    event_type = random.choice(event_types)
    event = {
        "user_id": f"U{random.randint(1000, 9999)}",
        "event_type": event_type,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ip": f"192.168.1.{random.randint(1, 255)}",
        "device":random.choice(devices),
    }
    event["user_type"] = random.choice(["new", "returning"])
    if event_type not in ["login", "logout"]:    
     event["product_id"] = f"P{random.randint(100, 999)}"
     event["product_category"] = random.choice(category)
     event["price"] = f"${random.randint(500, 9999)}"
    json_event = json.dumps(event)
    print(json_event)
    with open("events_log.json", "a") as file:
     file.write(json_event + "\n")
    time.sleep(1)
