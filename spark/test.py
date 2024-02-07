from pyspark.sql import SparkSession
from pyspark.sql.functions import split, array_contains, col
import time

# Initialiser la SparkSession
spark = SparkSession.builder.appName("OpenFoodFactsFilter").getOrCreate()

# Ajouter un timestamp au chemin de sauvegarde
epoch_timestamp = int(time.time())

# Chemin vers votre fichier CSV dans HDFS et chemin de sauvegarde avec timestamp
file_path = "hdfs://localhost:9000/user/hadoop/off_row/en.openfoodfacts.org.products.csv"
save_path_with_epoch_timestamp = f"hdfs://localhost:9000/user/hadoop/categories/chocolates_{epoch_timestamp}"

# Lire le fichier CSV
df = spark.read.csv(file_path, header=True, sep="\t")

# Convertir les catégories en array
df = df.withColumn("categories_tags_array", split(col("categories_tags"), ","))
df = df.withColumn("categories_en_array", split(col("categories_en"), ","))

# Filtre initial pour la France et les chocolats
filtered_df = df.filter(
    ((col("countries_en") == "France") | (col("countries_tags") == "en:france")) &
    (array_contains(col("categories_tags_array"), "en:chocolates") | array_contains(col("categories_en_array"), "Chocolates"))
)

# Exclure les lignes avec des informations manquantes dans les colonnes spécifiées
required_columns = ["quantity", "brands_tags", "categories", "categories_tags", "categories_en",
                    "countries", "countries_tags", "countries_en", "ecoscore_score", "unique_scans_n",
                    "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g",
                    "sugars_100g", "proteins_100g", "nutrition-score-fr_100g"]

for column in required_columns:
    filtered_df = filtered_df.filter(col(column).isNotNull() & (col(column) != ""))

# Sélectionner les colonnes spécifiques
selected_columns_df = filtered_df.select(
    "code", "creator", "last_modified_t", "last_modified_datetime", "last_modified_by",
    "last_updated_t", "last_updated_datetime", "product_name", "quantity", "brands_tags",
    "categories", "categories_tags", "categories_en", "labels", "labels_tags", "labels_en",
    "countries", "countries_tags", "countries_en", "food_groups", "food_groups_tags",
    "food_groups_en", "ecoscore_score", "unique_scans_n", "main_category", "main_category_en",
    "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g",
    "proteins_100g", "nutrition-score-fr_100g"
)

# Sauvegarder le DataFrame filtré
selected_columns_df.write.csv(save_path_with_epoch_timestamp, mode="overwrite", header=True)

spark.stop()
