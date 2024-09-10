import requests
import firebase_admin
from firebase_admin import db, credentials
import sys

# Set the default encoding to utf-8
sys.stdout.reconfigure(encoding='utf-8')

# API endpoint for products in India
url = "https://world.openfoodfacts.org/country/india.json"

# Define headers with a User-Agent
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def fetch_all_products(url):
    all_products = []
    page = 1
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
                
                all_products.extend(products)
                
                # If we fetch fewer products than the page size, it means we've reached the last page
                if len(products) < page_size:
                    break
                
                # Increment the page number
                page += 1
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                print(f"Response content: {response.text}")
                break
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    return all_products

# Fetch all products
all_products = fetch_all_products(url)

# Initialize Firebase
cred = credentials.Certificate("credentials.json")
firebase_admin.initialize_app(cred, {"databaseURL": "https://food-scanner-8021d-default-rtdb.asia-southeast1.firebasedatabase.app/"})

# Push data to Firebase
def push_to_firebase(products):
    # Create a reference to the Firebase Realtime Database
    ref = db.reference("/products")
    
    # Fetch the current number of products already in Firebase
    existing_products = ref.get()
    if existing_products:
        current_index = len(existing_products)  # Get the count of existing products
    else:
        current_index = 0  # Start from 0 if there are no products
    
    # Push each product to Firebase starting from the next available index
    for index, product in enumerate(products):
        ref.child(f"{current_index + index + 1}").set(product)

# Push the product data to Firebase
push_to_firebase(all_products)

print(f"Data has been successfully pushed to Firebase.")