import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def verify_data():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    db = client["ecommerce_db"]
    collection = db["Real time-Ecommerce"]
    
    print(f"Checking collection: {collection.name} in database: {db.name}")
    
    for i in range(5):
        data = collection.find_one({"_id": "latest_analytics"})
        if data:
            print(f"[{i+1}/5] Total Orders: {data.get('total_orders')}, Total Revenue: {data.get('total_revenue')}")
        else:
            print(f"[{i+1}/5] No data found yet...")
        time.sleep(3)

if __name__ == "__main__":
    verify_data()
