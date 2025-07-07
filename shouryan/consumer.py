import json
import time
import os
import pandas as pd
from datetime import datetime

class LocalFileConsumer:
    def __init__(self, file_path="events_log.json"):
        self.file_path = file_path
        self.data = pd.DataFrame(columns=["timestamp", "user_id", "event_type", "product_category"])
        self.running = False
        self.output_base = "event_partitions"

        # Create base output directory
        os.makedirs(self.output_base, exist_ok=True)

    def write_partitioned_event(self, event):
        try:
            event_type = event.get("event_type", "unknown")
            folder_path = os.path.join(self.output_base, event_type)
            os.makedirs(folder_path, exist_ok=True)

            filename = f"event_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}.json"
            full_path = os.path.join(folder_path, filename)

            # Make timestamp serializable
            if isinstance(event["timestamp"], pd.Timestamp):
                event["timestamp"] = event["timestamp"].isoformat()

            with open(full_path, "w") as f:
                json.dump(event, f)

            # Optionally append to a single CSV for cumulative data
            pd.DataFrame([event]).to_csv("events.csv", mode="a", header=not os.path.exists("events.csv"), index=False)

        except Exception as e:
            print(f"Error saving event: {e}")

    def read_stream(self):
        self.running = True
        print(f"Listening to {self.file_path}...\n")

        with open(self.file_path, "r") as f:
            # Move to end of file to simulate tailing new lines
            f.seek(0, 2)

            while self.running:
                line = f.readline()
                if not line:
                    time.sleep(0.1)
                    continue

                try:
                    event = json.loads(line)
                    event["timestamp"] = pd.to_datetime(event["timestamp"])

                    # Fix FutureWarning: match DataFrame columns
                    new_row = pd.DataFrame([event], columns=self.data.columns)
                    self.data = pd.concat([self.data, new_row], ignore_index=True)

                    print(f"Consumed: {event}")
                    self.write_partitioned_event(event)

                except json.JSONDecodeError:
                    print("Warning: Skipped malformed JSON")
                except Exception as e:
                    print(f"Error: {e}")

    def stop(self):
        self.running = False
        print("Stopped consuming.")

if __name__ == "__main__":
    consumer = LocalFileConsumer("events_log.json")
    try:
        consumer.read_stream()
    except KeyboardInterrupt:
        consumer.stop()
