from pyspark.sql import SparkSession, functions as F
import subprocess
import datetime
import time


hdfs_base_path = "hdfs://localhost:9000"

def run_cmd(cmd):
    """Run a shell command and return the output."""
    try:
        return subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e.output.decode('utf-8')}")
        return ""

def find_existing_data_dir():
    """Dynamically find the existing data directory."""
    cmd = "hdfs dfs -ls /user/hadoop/categories | grep 'chocolates_' | awk '{print $8}'"
    result = run_cmd(cmd)
    return result.splitlines()[0] if result else None

# Initialize Spark Session
spark = SparkSession.builder.appName("Process Chocolates Data").getOrCreate()

# Dynamically find the existing data directory
existing_data_dir = find_existing_data_dir()
print(f"Found existing data directory: {existing_data_dir}")

if not existing_data_dir.startswith(hdfs_base_path):
    existing_data_dir = f"{hdfs_base_path}{existing_data_dir}"
    
# Corrected Paths with HDFS scheme
cleaned_data_path = "hdfs://localhost:9000/user/hadoop/api_off_cleaned/chocolates"

# Read the cleaned data and filter
df_cleaned = spark.read.csv(f"{cleaned_data_path}/*.csv", header=True, inferSchema=True)

# Filter out rows with any empty fields in the specified columns
# Corrected columns_to_check with proper column names
columns_to_check = ["code", "last_updated_t", "product_name", "quantity", "brands_tags",
                    "categories", "categories_tags", "countries", "countries_tags",
                    "ecoscore_score", "unique_scans_n", "energy-kcal_100g", "fat_100g",
                    "saturated-fat_100g", "carbohydrates_100g", "sugars_100g", "proteins_100g",
                    "nutrition-score-fr_100g"]

df_filtered = df_cleaned.na.drop(subset=columns_to_check)

# Read the existing data
df_existing = spark.read.csv(f"{existing_data_dir}/*.csv", header=True, inferSchema=True)

# Log initial count
initial_count = df_existing.count()

# Merge and prioritize df_filtered records
df_merged = df_existing.join(df_filtered, "code", "outer").select(df_filtered["*"])

# Log final count and changes
final_count = df_merged.count()
print(f"Total lines after merge: {final_count}")
print(f"Lines appended or updated: {final_count - initial_count}")

# Write the merged DataFrame
temp_dir = f"{existing_data_dir}_temp"
df_merged.write.csv(temp_dir, mode="overwrite", header=True)

# Clean up and move
run_cmd(f"hdfs dfs -rm -r {existing_data_dir}")
run_cmd(f"hdfs dfs -mv {temp_dir} {existing_data_dir}")

# Rename with Unix epoch timestamp
epoch_timestamp = str(int(time.time()))
new_dir_name = f"hdfs://localhost:9000/user/hadoop/categories/chocolates_{epoch_timestamp}"
run_cmd(f"hdfs dfs -mv {existing_data_dir} {new_dir_name}")
print(f"Directory renamed to: {new_dir_name}")

spark.stop()
