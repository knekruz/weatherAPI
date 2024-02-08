CREATE TABLE IF NOT EXISTS chocolates (
    code STRING,
    last_updated_t STRING,
    product_name STRING,
    quantity STRING,
    brands_tags STRING,
    categories STRING,
    categories_tags STRING,
    countries STRING,
    countries_tags STRING,
    ecoscore_score STRING,
    unique_scans_n STRING,
    energy_kcal_100g STRING,
    fat_100g STRING,
    saturated_fat_100g STRING,
    carbohydrates_100g STRING,
    sugars_100g STRING,
    proteins_100g STRING,
    nutrition_score_fr_100g STRING,
    creator STRING,
    last_modified_by STRING,
    last_updated_datetime STRING,
    labels STRING,
    labels_tags STRING,
    food_groups STRING,
    food_groups_tags STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
