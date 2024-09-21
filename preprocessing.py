import requests
from pymongo import MongoClient
import time

# Connect to MongoDB
client = MongoClient('mongodb+srv://packagedfoodanalysis:Ezk77HCR20JBULwo@cluster0.zzj38.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')  # Replace with your MongoDB connection string
db = client['openfoodfacts']  
collection = db['products']  

def missing_ingredients():
    # Build the query to find documents where 'ingredients' is missing or null
    query = {
        'ingredients': {'$exists': False}  # Find documents where 'ingredients' does not exist
    }

    # Fetch documents that match the query and get their _id fields
    matching_docs = collection.find(query, {'_id': 1})
    ids_with_issues = [doc['_id'] for doc in matching_docs]

    # Count the number of documents with missing ingredients
    count_with_issues = len(ids_with_issues)

    # Print the results
    print(f"Number of products missing ingredients: {count_with_issues}")
    
    # Delete documents that are missing ingredients
    result = collection.delete_many({'_id': {'$in': ids_with_issues}})
    print(f"Number of deleted products: {result.deleted_count}")

def remove_redundant_data():
    fields_to_remove = {
    "categories_properties": "",
    "category_properties": "",
    "categories_properties_tags": "",
    "categories_old": "",
    "checkers_tags": "",
    "ciqual_food_name_tags": "",
    "correctors_tags": "",
    "complete": "",
    "completeness": "",
    "created_t": "",
    "creator": "",
    "data_quality_info_tags": "",
    "data_quality_tags": "",
    "data_quality_bugs_tags": "",
    "data_quality_errors_tags": "",
    "data_quality_warnings_tags": "",
    "data_sources": "",
    "data_sources_tags": "",
    "ecoscore_data.transportation_score": "",
    "ecoscore_data.transportation_scores": "",
    "ecoscore_data.transportation_value": "",
    "ecoscore_data.transportation_values": "",
    "ecoscore_data.value": "",
    "ecoscore_data.values": "",
    "ecoscore_data.agribalyse": "",
    "ecoscore_data.missing_data_warning": "",
    "ecoscore_data.previous_data": "",
    "ecoscore_data.missing": "",
    "ecoscore_data.production_system": "",
    "ecoscore_data.missing_key_data": "",
    "ecoscore_extended_data_version": "",
    "editors": "",
    "editors_tags": "",
    "emb_codes": "",
    "emb_codes_20141016": "",
    "emb_codes_orig": "",
    "entry_dates_tags": "",
    "expiration_date": "",
    "images": "",
    "informers_tags": "",
    "interface_version_created": "",
    "interface_version_modified": "",
    "labels_old": "",
    "last_edit_dates_tags": "",
    "last_editor": "",
    "last_image_dates_tags": "",
    "last_image_t": "",
    "last_modified_by": "",
    "last_modified_t": "",
    "last_updated_t": "",
    "max_imgid": "",
    "misc_tags": "",
    "nutriscore_version": "",
    "packaging_old": "",
    "packaging_old_before_taxonomization": "",
    "packaging_recycling_tags": "",
    "photographers_tags": "",
    "popularity_tags": "",
    "rev": "",
    "removed_countries_tags": "",
    "scans_n": "",
    "states": "",
    "states_hierarchy": "",
    "states_tags": "",
    "stores": "",
    "stores_tags": "",
    "teams": "",
    "teams_tags": "",
    "unique_scans_n": "",
    "update_key": ""
}

# Use the $unset operator to remove the fields
    collection.update_many({}, {"$unset": fields_to_remove})

    print("Specified fields have been removed from all documents.")

# Call the function to delete documents missing ingredients
missing_ingredients()
remove_redundant_data()
