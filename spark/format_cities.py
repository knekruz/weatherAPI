from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import subprocess
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("FilterCitiesData").getOrCreate()

hdfs_base_path = "hdfs://localhost:9000"  # HDFS base path
local_base_path = "/home/hadoop/Desktop/ECBD/output/formatted"  # Local save path

def list_hdfs_directories(base_path):
    """Use subprocess to list directories in HDFS."""
    result = subprocess.run(['hdfs', 'dfs', '-ls', base_path], capture_output=True, text=True)
    if result.returncode == 0:
        lines = result.stdout.split('\n')
        directories = [line.split()[-1] for line in lines if line.startswith('d') and line]
        return [path for path in directories]
    else:
        print("Error listing directories:", result.stderr)
        return []

def process_city_data(country_name):
    """Process city data, apply filtering, and save to HDFS and locally."""
    input_path = f"{hdfs_base_path}/user/hadoop/weather/raw/{country_name}/cities.json"
    output_path_hdfs = f"{hdfs_base_path}/user/hadoop/weather/formatted/{country_name}"
    output_path_local = os.path.join(local_base_path, country_name)
    
    df = spark.read.json(input_path, multiLine=True)

    # Utilisation des fonctions de fenêtrage (Window functions)
    windowSpec = Window.partitionBy("country").orderBy("name")
    df = df.withColumn("row_num", F.row_number().over(windowSpec))

    # Utilisation d'une requête SQL (SQL query)
    df.createOrReplaceTempView("cities")
    df_final = spark.sql("""
        SELECT name, lat, lon, country, state
        FROM cities
    """)

    # Ensure directories exist and save filtered data
    os.makedirs(output_path_local, exist_ok=True)
    df_final.write.mode("overwrite").json(output_path_hdfs)
    df_final.coalesce(1).write.json(output_path_local, mode="overwrite")

if __name__ == "__main__":
    if list_hdfs_directories(f"{hdfs_base_path}/user/hadoop/weather/raw"):
        countries_directories = list_hdfs_directories(f"{hdfs_base_path}/user/hadoop/weather/raw")
        for country_path in countries_directories:
            country_name = os.path.basename(country_path)
            print(f"Processing data for country: {country_name}")
            process_city_data(country_name)
    else:
        print("HDFS is not accessible or not running.")

    spark.stop()
