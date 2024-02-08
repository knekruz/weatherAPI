from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("OFF Data Processing").getOrCreate()

# HDFS base path
hdfs_base_path = "hdfs://localhost:9000"

# Load JSON data
json_path = f"{hdfs_base_path}/user/hadoop/api_off_raw/categories/chocolates.json"
df = spark.read.json(json_path)

# Extract timestamp from HDFS folder name
folder_timestamp = 1672923569  # Example timestamp, replace with dynamic extraction if necessary

# Filter records based on last_updated_t
filtered_df = df.filter(col("last_updated_t") > lit(folder_timestamp))

# Transform JSON to match CSV schema
transformed_df = filtered_df.select(
    col("code"),
    col("creator"),
    col("last_modified_t"),
    col("last_modified_datetime"),
    col("last_modified_by"),
    col("last_updated_t"),
    col("last_updated_datetime"),
    col("product_name"),
    col("quantity"),
    F.concat_ws(",", col("brands_tags")).alias("brands_tags"),
    col("categories"),
    F.concat_ws(",", col("categories_tags")).alias("categories_tags"),
    F.concat_ws(",", col("categories_en")).alias("categories_en"),
    F.concat_ws(",", col("labels")).alias("labels"),
    F.concat_ws(",", col("labels_tags")).alias("labels_tags"),
    F.concat_ws(",", col("labels_en")).alias("labels_en"),
    F.concat_ws(",", col("countries")).alias("countries"),
    F.concat_ws(",", col("countries_tags")).alias("countries_tags"),
    F.concat_ws(",", col("countries_en")).alias("countries_en"),
    F.concat_ws(",", col("food_groups")).alias("food_groups"),
    F.concat_ws(",", col("food_groups_tags")).alias("food_groups_tags"),
    F.concat_ws(",", col("food_groups_en")).alias("food_groups_en"),
    col("ecoscore_score"),
    col("unique_scans_n"),
    col("main_category"),
    col("main_category_en"),
    col("energy-kcal_100g"),
    col("fat_100g"),
    col("saturated-fat_100g"),
    col("carbohydrates_100g"),
    col("sugars_100g"),
    col("proteins_100g"),
    col("nutrition-score-fr_100g")
)

# Save the transformed DataFrame to HDFS in CSV format
csv_path = f"{hdfs_base_path}/user/hadoop/api_off_cleaned/chocolates.csv"
transformed_df.write.csv(csv_path, mode="overwrite", header=True)

print("Data transformation and saving to HDFS completed.")
