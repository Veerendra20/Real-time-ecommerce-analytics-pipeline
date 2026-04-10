import json
from kafka import KafkaConsumer
from collections import defaultdict
import os
import time
from pymongo import MongoClient

# Load environment variables (Optional dependency)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def clear_console():
    """Clears the console screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    """Reads orders from Kafka and performs real-time analytics."""
    
    # --- KAFKA CONNECTION (WITH RETRIES) ---
    consumer = None
    retry_count = 0
    max_retries = 10
    
    while retry_count < max_retries:
        print(f"Connecting to Kafka Broker (localhost:9092)... Attempt {retry_count + 1}/{max_retries}")
        try:
            consumer = KafkaConsumer(
                'orders',
                bootstrap_servers=['127.0.0.1:9092'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("[SUCCESS] Successfully connected to Kafka!")
            break
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"[ERROR] Failed to connect to Kafka after {max_retries} attempts: {e}")
                return
            time.sleep(3)

    # --- MONGODB CONNECTION ---
    mongo_uri = os.getenv('MONGO_URI')
    
    if not mongo_uri:
        print("[ERROR] DATABASE CONFIGURATION MISSING!")
        print("Please ensure MONGO_URI (Atlas Cloud) is set in your environment or .env file.")
        print("Local fallbacks are disabled for this 'Atlas-Only' deployment.")
        return

    print("Connecting to MongoDB Atlas Cloud Cluster...")
    try:
        mongo_client = MongoClient(mongo_uri)
        db = mongo_client["ecommerce_db"]
        collection = db["Real time-Ecommerce"]
        # Force connection check
        mongo_client.admin.command('ping')
        print("[SUCCESS] Successfully connected to MongoDB Atlas!")
    except Exception as e:
        print(f"[ERROR] Fatal error connecting to Atlas: {e}")
        return

    # Analytics variables
    total_revenue = 0.0
    total_orders = 0
    product_counts = defaultdict(int)
    category_counts = defaultdict(int)
    payment_counts = defaultdict(int)
    recent_transactions = []

    print("Listening for successful transactions on topic 'orders'...")

    try:
        for message in consumer:
            try:
                order = message.value
                
                # Filter for successful transactions
                if order.get("order_status") == "Success":
                    total_revenue += order.get("amount", 0.0)
                    total_orders += 1
                    product_counts[order.get("product", "Unknown")] += 1
                    category_counts[order.get("category", "Unknown")] += 1
                    payment_counts[order.get("payment_method", "Unknown")] += 1
                    
                    # Keep track of last 10 transactions
                    recent_transactions.insert(0, order)
                    if len(recent_transactions) > 10:
                        recent_transactions.pop()
                    
                    # Calculate Average Order Value
                    aov = total_revenue / total_orders if total_orders > 0 else 0
                    
                    # Live Dashboard View (Console)
                    if not is_cloud:
                        clear_console()
                        print("="*40)
                        print("   LIVE E-COMMERCE ANALYTICS DASHBOARD")
                        print("="*40)
                        print(f"Total Successful Orders: {total_orders}")
                        print(f"Total Revenue:         INR {total_revenue:,.2f}")
                        print(f"Average Order Value:   INR {aov:,.2f}")
                        print("-"*40)
                        print("Top Categories:")
                        for cat, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
                            print(f" - {cat:12}: {count}")
                        print("="*40)

                    # Save analytics to MongoDB
                    analytics_data = {
                        "total_revenue": total_revenue,
                        "total_orders": total_orders,
                        "avg_order_value": aov,
                        "product_counts": dict(product_counts),
                        "category_counts": dict(category_counts),
                        "payment_counts": dict(payment_counts),
                        "recent_transactions": recent_transactions,
                        "last_updated": order.get("timestamp")
                    }
                    collection.update_one(
                        {"_id": "latest_analytics"},
                        {"$set": analytics_data},
                        upsert=True
                    )
            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("\nAnalytics consumer stopped.")
    except Exception as e:
        print(f"Fatal error in consumer: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
