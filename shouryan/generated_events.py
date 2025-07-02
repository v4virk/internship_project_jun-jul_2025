import time
import json
import random
from datetime import datetime
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
    event_type = random.choice(event_types)
    event = {
        "user_id": f"U{random.randint(1000, 9999)}",
        "event_type": event_type,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ip": f"192.168.1.{random.randint(1, 255)}",
        "device": random.choice(devices),
        "user_type": random.choice(["new", "returning"])
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
    with open("events_log.json", "a", encoding="utf-8") as file:
        file.write(json_event + "\n")
        file.flush()
    time.sleep(0.01)
