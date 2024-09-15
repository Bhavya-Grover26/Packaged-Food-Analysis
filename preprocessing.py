import requests
from pymongo import MongoClient
import time

# Connect to MongoDB
client = MongoClient('mongodb+srv://packagedfoodanalysis:Ezk77HCR20JBULwo@cluster0.zzj38.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')  # Replace with your MongoDB connection string
db = client['openfoodfacts']  # Replace with your database name
collection = db['products']  # Replace with your collection name

def missing_ingredients():
    fields = [
        'ingredients_text',
        'ingredients_tags',
        'ingredients_original_tags'
    ]

    # Build the query to find documents where any of the fields are empty strings, empty arrays, or null
    query = {
        '$or': [
            {field: {'$in': ['']}} for field in fields
        ]
    }

    # Fetch documents that match the query and get their _id fields
    matching_docs = collection.find(query, {'_id': 1})
    ids_with_issues = [doc['_id'] for doc in matching_docs]

    # Count the number of documents with issues
    count_with_issues = len(ids_with_issues)

    # Print the results
    print(f"Number of products with issues: {count_with_issues}")
    print("Product IDs with issues:")
    #print(ids_with_issues)

missing_ingredients()