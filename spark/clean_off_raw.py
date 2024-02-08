from pyspark.sql import SparkSession
from pyspark.sql.functions import split, array_contains, col
import time

# Initialize SparkSession
spark = SparkSession.builder.appName("OpenFoodFactsFilter").getOrCreate()

# Timestamp for the save path
epoch_timestamp = int(time.time())

# File path in HDFS and save path with timestamp
file_path = "hdfs://localhost:9000/user/hadoop/off_row/en.openfoodfacts.org.products.csv"
save_path_with_epoch_timestamp = f"hdfs://localhost:9000/user/hadoop/categories/chocolates_1672923569"

# Read the CSV file
df = spark.read.csv(file_path, header=True, sep="\t")

# Convert categories, countries, and countries_tags to arrays
df = df.withColumn("categories_tags_array", split(col("categories_tags"), ","))
df = df.withColumn("categories_en_array", split(col("categories"), ","))
df = df.withColumn("countries_array", split(col("countries"), ","))
df = df.withColumn("countries_tags_array", split(col("countries_tags"), ","))

# Initial filter for France and chocolates
filtered_df = df.filter(
    (array_contains(col("countries_array"), "France") | array_contains(col("countries_tags_array"), "en:france")) &
    (array_contains(col("categories_tags_array"), "en:chocolates") | array_contains(col("categories_en_array"), "Chocolates"))
)

# Exclude rows missing information in specified columns
required_columns = ["code", "last_updated_t", "product_name", "quantity", "brands_tags", "categories",
                    "categories_tags", "countries", "countries_tags", "ecoscore_score", "unique_scans_n",
                    "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g",
                    "proteins_100g", "nutrition-score-fr_100g"]

for column in required_columns:
    filtered_df = filtered_df.filter(col(column).isNotNull() & (col(column) != ""))

# Select specific columns
selected_columns_df = filtered_df.select(
    "code", "creator", "last_modified_by", "last_updated_t", "last_updated_datetime",
                    "product_name", "quantity", "brands_tags", "categories", "categories_tags", "labels",
                    "labels_tags", "countries", "countries_tags", "food_groups", "food_groups_tags",
                    "ecoscore_score", "unique_scans_n","energy-kcal_100g", "fat_100g",
                    "saturated-fat_100g", "carbohydrates_100g", "sugars_100g", "proteins_100g",
                    "nutrition-score-fr_100g"
)

# Save the filtered DataFrame
selected_columns_df.write.csv(save_path_with_epoch_timestamp, mode="overwrite", header=True)

spark.stop()
