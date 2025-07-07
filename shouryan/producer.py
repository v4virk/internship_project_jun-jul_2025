import time
import json
import random
from datetime import datetime

def generate_event():
    events = ['login', 'view_product', 'add_to_cart', 'purchase', 'logout']
    categories = ['electronics', 'clothing', 'home', 'books', 'beauty']
    return {
        'timestamp': datetime.now().isoformat(),
        'user_id': f"user_{random.randint(1000, 9999)}",
        'event_type': random.choice(events),
        'product_category': random.choice(categories)
    }

if __name__ == "__main__":
    with open("events_log.json", "a") as file:
        while True:
            event = generate_event()
            file.write(json.dumps(event) + "\n")
            file.flush()  # ensures data is written immediately
            print(f"Produced: {event}")
            time.sleep(1) 