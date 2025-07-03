%matplotlib inline
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import time
import random
from datetime import datetime, timedelta
import sys

class RealTimeAnalytics:
    def __init__(self):
        self.data = pd.DataFrame(columns=['timestamp', 'user_id', 'event_type', 'product_category'])
        self.running = False
        self.event_count = 0
        self.update_interval = 0.5
        self.events_per_second = 60
        self.event_interval = 1.0 / self.events_per_second

    def generate_event(self):
        events = ['login', 'view_product', 'add_to_cart', 'purchase', 'logout']
        categories = ['electronics', 'clothing', 'home', 'books', 'beauty']
        return {
            'timestamp': datetime.now(),
            'user_id': f"user_{random.randint(1000, 9999)}",
            'event_type': random.choice(events),
            'product_category': random.choice(categories)
        }

    def classify_journey(self, events):
        events = [e.lower() for e in events]
        if 'login' in events:
            if 'purchase' in events:
                return 'Completed Purchase'
            elif 'add_to_cart' in events:
                return 'Abandoned Cart'
            return 'Browsing Only'
        return 'Other'

    def update_visualizations(self):
        from IPython.display import clear_output
        clear_output(wait=True)

        fig, ax = plt.subplots(4, 2, figsize=(18, 18))
        fig.suptitle("Real-Time User Journey Analytics", fontsize=18)

        time_threshold = datetime.now() - timedelta(seconds=60)
        recent_data = self.data[self.data['timestamp'] > time_threshold]

        # 1. User Journey Funnel
        user_paths = recent_data.groupby('user_id')['event_type'].apply(list).reset_index()
        user_paths['journey_type'] = user_paths['event_type'].apply(self.classify_journey)
        journey_counts = user_paths['journey_type'].value_counts()
        sns.barplot(x=journey_counts.values, y=journey_counts.index, hue=journey_counts.index, palette='viridis', ax=ax[0, 0], legend=False)
        ax[0, 0].set_title('User Journey Funnel (Last 60s)')
        ax[0, 0].set_xlabel('User Count')

        # 2. Event Frequency
        event_counts = recent_data['event_type'].value_counts()
        event_counts.plot(kind='bar', ax=ax[0, 1], color='teal')
        ax[0, 1].set_title('Event Frequency')
        ax[0, 1].set_ylabel('Count')

        # 3. Category Distribution
        category_counts = recent_data['product_category'].value_counts()
        category_counts.plot(kind='pie', ax=ax[1, 0], autopct='%1.1f%%')
        ax[1, 0].set_title('Product Categories')

        # 4. Events Per Second
        time_series = recent_data.set_index('timestamp').resample('500ms').count()['user_id']
        if not time_series.empty:
            time_series.plot(ax=ax[1, 1], marker='o', color='orange')
            ax[1, 1].set_title('Events Per Second')
            ax[1, 1].set_ylabel('Events')
        else:
            ax[1, 1].text(0.5, 0.5, 'Waiting for data...', ha='center', va='center')

        # 5. Top 5 Active Users
        top_users = recent_data['user_id'].value_counts().nlargest(5)
        sns.barplot(x=top_users.values, y=top_users.index, ax=ax[2, 0])
        ax[2, 0].set_title('Top 5 Active Users')
        ax[2, 0].set_xlabel('Event Count')

        # 6. Heatmap: Category vs Event Type
        category_event_matrix = pd.crosstab(recent_data['product_category'], recent_data['event_type'])
        sns.heatmap(category_event_matrix, annot=True, fmt='d', cmap='YlGnBu', ax=ax[2, 1])
        ax[2, 1].set_title('Heatmap: Category vs Event Type')

        # 7. Event Types Over Time (Stacked Area)
        event_time_series = recent_data.set_index('timestamp').groupby([pd.Grouper(freq='5s'), 'event_type']).size().unstack(fill_value=0)
        if len(event_time_series) > 1:
            event_time_series.plot.area(ax=ax[3, 0], stacked=True, alpha=0.6)
            ax[3, 0].set_title('Event Type Over Time (Stacked Area)')
            ax[3, 0].set_ylabel('Count')
        else:
            ax[3, 0].text(0.5, 0.5, 'Not enough data', ha='center', va='center')

        # 8. Top Product Categories Over Time (Line Plot)
        cat_time_series = recent_data.set_index('timestamp').groupby([pd.Grouper(freq='5s'), 'product_category']).size().unstack(fill_value=0)
        if len(cat_time_series) > 1:
            cat_time_series.plot(ax=ax[3, 1], marker='o')
            ax[3, 1].set_title('Top Product Categories Over Time')
            ax[3, 1].set_ylabel('Count')
        else:
            ax[3, 1].text(0.5, 0.5, 'Not enough data', ha='center', va='center')

        plt.tight_layout()
        plt.show()

        print(f"Events: {len(self.data)} | Last 60s: {len(recent_data)} | Rate: {self.events_per_second} eps", flush=True)

    def data_generation_loop(self, max_events=3600):
        last_update = time.time()
        while self.running and self.event_count < max_events:
            start_time = time.time()
            new_event = self.generate_event()
            new_df = pd.DataFrame([new_event])
            if not self.data.empty:
                self.data = pd.concat([self.data, new_df], ignore_index=True)
            else:
                self.data = new_df.copy()
            self.event_count += 1

            current_time = time.time()
            if current_time - last_update >= self.update_interval:
                self.update_visualizations()
                last_update = current_time

            elapsed = time.time() - start_time
            sleep_time = self.event_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    def start(self, events_per_second=60, max_events=3600):
        self.events_per_second = events_per_second
        self.event_interval = 1.0 / self.events_per_second
        self.running = True
        print(f"Started real-time analytics at {events_per_second} events/second", flush=True)
        self.data_generation_loop(max_events=max_events)

    def stop(self):
        self.running = False
        plt.close('all')
        print("\nStopped real-time analytics", flush=True)

if __name__ == "__main__":
    try:
        import pandas, numpy, matplotlib, seaborn
    except ImportError:
        !pip install pandas numpy matplotlib seaborn

    analyzer = RealTimeAnalytics()
    try:
        analyzer.start(events_per_second=60, max_events=600)
    except KeyboardInterrupt:
        analyzer.stop()
