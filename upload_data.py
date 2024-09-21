import requests
from pymongo import MongoClient
import time

# API endpoint for products in India
url = "https://world.openfoodfacts.org/country/france.json"

# Define headers with a User-Agent
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# MongoDB Atlas connection setup
client = MongoClient("mongodb+srv://packagedfoodanalysis:Ezk77HCR20JBULwo@cluster0.zzj38.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['openfoodfacts']  # Database name
collection = db['products_with_france']   # Collection name

def fetch_and_store_products(url):
    page = 92
    page_size = 100  # The Open Food Facts API may have a limit on page size

    while True:
        try:
            # Send GET request to Open Food Facts API with headers and pagination
            response = requests.get(url, headers=headers, params={"page": page, "page_size": page_size})
            
            # Check the status code of the response
            if response.status_code == 200:
                data = response.json()
                
                # Extract products
                products = data.get('products', [])
                if not products:
                    break  # No more products to fetch
                
                # Insert or update products in MongoDB
                for product in products:
                    # Use update_one with upsert=True to handle duplicates
                    collection.update_one(
                        {"_id": product["_id"]},
                        {"$set": product},
                        upsert=True
                    )
                
                print(f"Processed {len(products)} products from page {page}.")
                
                # If fewer products than page_size are fetched, break (last page)
                if len(products) < page_size:
                    break
                
                # Increment the page number
                page += 1

                # Add a delay to avoid hitting rate limits
                time.sleep(10)  # Sleep for 10 seconds
            elif response.status_code == 429:
                print("Rate limit exceeded. Sleeping for 60 seconds...")
                time.sleep(60)  # Sleep for 60 seconds if rate limit exceeded
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                print(f"Response content: {response.text}")
                break
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

# Fetch all products and store them in MongoDB
fetch_and_store_products(url)
