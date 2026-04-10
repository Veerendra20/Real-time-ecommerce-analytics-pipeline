import random
import time
import json
from datetime import datetime
from kafka import KafkaProducer

# Configuration for fake data
PRODUCTS = ["Laptop", "Phone", "Shoes", "Watch", "Headphones"]
PRODUCT_CATEGORIES = {
    "Laptop": "Electronics",
    "Phone": "Electronics",
    "Shoes": "Fashion",
    "Watch": "Accessories",
    "Headphones": "Accessories"
}
CITIES = ["Delhi", "Mumbai", "Bangalore", "Hyderabad", "Chennai"]
PAYMENT_METHODS = ["UPI", "Card", "COD"]
ORDER_STATUSES = ["Success", "Failed"]

def generate_order():
    """Generates a random e-commerce transaction."""
    product = random.choice(PRODUCTS)
    category = PRODUCT_CATEGORIES[product]
    
    order = {
        "user_id": random.randint(1000, 9999),
        "product": product,
        "category": category,
        "amount": round(random.uniform(500, 50000), 2),
        "city": random.choice(CITIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return order

def main():
    """Continuously sends fake order data to Kafka every second."""
    print("Connecting to Kafka Broker (127.0.0.1:9092)...")
    producer = None
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['127.0.0.1:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[SUCCESS] Successfully connected to Kafka!")
            break
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"[ERROR] Failed to connect to Kafka after {max_retries} attempts: {e}")
                return
            print(f"[RETRY] Attempt {retry_count} failed, retrying in 3s...")
            time.sleep(3)

    print("Starting e-commerce transaction stream to Kafka topic 'orders'... (Press Ctrl+C to stop)")
    try:
        while True:
            try:
                order = generate_order()
                # Send message to Kafka
                producer.send('orders', order)
                producer.flush() # Ensure message is sent
                
                print(f"Sent: {json.dumps(order)}")
            except Exception as e:
                print(f"Failed to send message to Kafka: {e}")
            
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSimulation stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
