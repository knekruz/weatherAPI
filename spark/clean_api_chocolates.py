from pyspark.sql import SparkSession, functions as F
import subprocess
import re

# Initialize Spark Session
spark = SparkSession.builder.appName("JSON to CSV Transformation").getOrCreate()

# HDFS base path
hdfs_base_path = "hdfs://localhost:9000"

# Function to dynamically retrieve the epoch timestamp from the folder name using HDFS command
def get_latest_chocolates_timestamp(hdfs_path):
    # Execute HDFS command to list directories
    cmd = f"hdfs dfs -ls {hdfs_path}"
    result = subprocess.run(cmd.split(), capture_output=True, text=True)
    # Use regular expression to find the folder that matches the pattern 'chocolates_'
    match = re.findall(r'chocolates_(\d+)', result.stdout)
    # Return the latest timestamp if multiple matches found
    if match:
        return max(int(timestamp) for timestamp in match)
    else:
        raise ValueError("No matching 'chocolates_' directory found")

# Retrieve the epoch timestamp dynamically
folder_path = "/user/hadoop/categories"
epoch_timestamp = get_latest_chocolates_timestamp(folder_path)

# Path to the JSON file in HDFS
json_file_path = f"{hdfs_base_path}/user/hadoop/api_off_raw/categories/chocolates.json"

# Read the JSON file
df = spark.read.json(json_file_path)

# Filter rows where last_updated_t is greater than or equal to the folder timestamp
filtered_df = df.filter(F.col("last_updated_t") >= epoch_timestamp)

# Select and transform columns as per the CSV format, including formatting the datetime
transformed_df = filtered_df.select(
    F.col("code"),
    F.col("creator"),
    F.col("last_modified_by"),
    F.col("last_updated_t"),
    F.from_unixtime("last_updated_t", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("last_updated_datetime"),
    F.col("product_name"),
    F.col("quantity"),
    F.concat_ws(",", F.col("brands_tags")).alias("brands_tags"),
    F.col("categories"),
    F.concat_ws(",", F.col("categories_tags")).alias("categories_tags"),
    F.col("labels"),
    F.concat_ws(",", F.col("labels_tags")).alias("labels_tags"),
    F.col("countries"),
    F.concat_ws(",", F.col("countries_tags")).alias("countries_tags"),
    F.col("food_groups"),
    F.concat_ws(",", F.col("food_groups_tags")).alias("food_groups_tags"),
    F.col("ecoscore_score"),
    F.col("unique_scans_n"),
    F.col("energy-kcal_100g"),
    F.col("fat_100g"),
    F.col("saturated-fat_100g"),
    F.col("carbohydrates_100g"),
    F.col("sugars_100g"),
    F.col("proteins_100g"),
    F.col("nutrition-score-fr_100g")
)

# Save the transformed DataFrame to CSV in HDFS
csv_file_path = f"{hdfs_base_path}/user/hadoop/api_off_cleaned/chocolates"
transformed_df.write.option("header", True).mode("overwrite").csv(csv_file_path)

# Stop the Spark session
spark.stop()
