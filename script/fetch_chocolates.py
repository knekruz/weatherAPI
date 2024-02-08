import requests
import json
import subprocess

def fetch_all_pages(endpoint, initial_params):
    all_products = []
    page = 1
    page_count = 1  # Initial value to enter the loop

    while page <= page_count:
        # Update page number in parameters
        params = initial_params.copy()
        params['page'] = page

        # Fetch data from API
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            data = response.json()
            page_count = data['page_count']  # Update total page count based on response
            all_products.extend(data['products'])
            print(f"Fetched page {page} of {page_count}.")
        else:
            print(f"Failed to fetch page {page}. Status code: {response.status_code}")
            break  # Exit loop if a page request fails

        page += 1

    return all_products

# API endpoint and parameters
api_endpoint = "https://world.openfoodfacts.net/api/v2/search"
params = {
    'categories_tags': 'chocolates',
    'countries_tags': 'en:france',
    'fields': 'code,creator,last_modified_by,last_updated_t,last_updated_datetime,product_name,quantity,brands_tags,categories,categories_tags,labels,labels_tags,countries,countries_tags,food_groups,food_groups_tags,ecoscore_score,unique_scans_n,main_category,energy-kcal_100g,fat_100g,saturated-fat_100g,carbohydrates_100g,sugars_100g,proteins_100g,nutrition-score-fr_100g'
}

# Fetch all products
products = fetch_all_pages(api_endpoint, params)

# Save the data to a file
file_path = 'chocolates.json'
with open(file_path, 'w') as file:
    json.dump(products, file)
print("Data fetched and saved locally.")

# Move the file to HDFS
hdfs_path = '/user/hadoop/api_off_raw/categories/chocolates.json'
subprocess.run(['hdfs', 'dfs', '-put', '-f', file_path, hdfs_path], check=True)
print(f"File moved to HDFS: {hdfs_path}")
