# Databricks notebook source
# Replace <access-key> with the key your access key
import os

storage_account_name = "tokyoolympicdata05"
container_name = "tokyo-olympic-data"
access_key = os.getenv("AZURE_ACCESS_KEY")

# Set the configuration
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    access_key
)

print("✓ Configuration set successfully!")

# COMMAND ----------

# Test if connection works by listing files in raw folder
dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw-data/")


# COMMAND ----------

# Read Athletes data
athletes_df = spark.read.csv(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw-data/athletes.csv",
    header=True,
    inferSchema=True
)

# Read Coaches data
coaches_df = spark.read.csv(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw-data/coaches.csv",
    header=True,
    inferSchema=True
)

# Read EntriesGender data
entries_gender_df = spark.read.csv(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw-data/entriesgender.csv",
    header=True,
    inferSchema=True
)

# Read Medals data
medals_df = spark.read.csv(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw-data/medals.csv",
    header=True,
    inferSchema=True
)
# Read Medals data
teams_df = spark.read.csv(
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw-data/teams.csv",
    header=True,
    inferSchema=True
)
print("✓ All data loaded successfully!")

# COMMAND ----------

# Display top 5 rows of Athletes dataframe
print("=== ATHLETES - Top 5 Rows ===")
athletes_df.show(5)

# Display top 5 rows of Coaches dataframe
print("\n=== COACHES - Top 5 Rows ===")
coaches_df.show(5)

# Display top 5 rows of EntriesGender dataframe
print("\n=== ENTRIES GENDER - Top 5 Rows ===")
entries_gender_df.show(5)

# Display top 5 rows of Medals dataframe
print("\n=== MEDALS - Top 5 Rows ===")
medals_df.show(5)

# Display top 5 rows of Medals dataframe
print("\n=== TEAMS - Top 5 Rows ===")
teams_df.show(5)

# COMMAND ----------

athletes_df.printSchema()
coaches_df.printSchema()
entries_gender_df.printSchema()
medals_df.printSchema()
teams_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

# Function to count nulls in each column
def count_nulls(df, df_name):
    print(f"\n=== NULL COUNT FOR {df_name} ===")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ])
    null_counts.show()

# Check nulls for all dataframes
count_nulls(athletes_df, "ATHLETES")
count_nulls(coaches_df, "COACHES")
count_nulls(entries_gender_df, "ENTRIES GENDER")
count_nulls(medals_df, "MEDALS")
count_nulls(teams_df, "TEAMS")

# COMMAND ----------

# Athletes
print(f"Athletes: {athletes_df.count()} rows, {len(athletes_df.columns)} columns")

# Coaches
print(f"Coaches: {coaches_df.count()} rows, {len(coaches_df.columns)} columns")

# Entries Gender
print(f"Entries Gender: {entries_gender_df.count()} rows, {len(entries_gender_df.columns)} columns")

# Medals
print(f"Medals: {medals_df.count()} rows, {len(medals_df.columns)} columns")

# Teams
print(f"Teams: {teams_df.count()} rows, {len(teams_df.columns)} columns")


# COMMAND ----------

# Replace null values with "Not Assigned" or "Unknown"
from pyspark.sql.functions import col, when

coaches_df_cleaned = coaches_df.fillna({"Event": "Not Assigned"})
coaches_df_cleaned.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

# Function to count nulls in each column
def count_nulls(df, df_name):
    print(f"\n=== NULL COUNT FOR {df_name} ===")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ])
    null_counts.show()

# Check nulls for all dataframes
count_nulls(athletes_df, "ATHLETES")
count_nulls(coaches_df_cleaned, "COACHES")
count_nulls(entries_gender_df, "ENTRIES GENDER")
count_nulls(medals_df, "MEDALS")
count_nulls(teams_df, "TEAMS")

# COMMAND ----------

# Check duplicates for each dataframe
def check_duplicates(df, df_name):
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    duplicate_rows = total_rows - distinct_rows
    
    print(f"{df_name}:")
    print(f"  Total Rows: {total_rows}")
    print(f"  Distinct Rows: {distinct_rows}")
    print(f"  Duplicate Rows: {duplicate_rows}")
    print()

# Check all dataframes
check_duplicates(athletes_df, "Athletes")
check_duplicates(coaches_df_cleaned, "Coaches")
check_duplicates(entries_gender_df, "Entries Gender")
check_duplicates(medals_df, "Medals")
check_duplicates(teams_df, "Teams")

# COMMAND ----------

# Remove duplicates from all dataframes
athletes_df_clean = athletes_df.dropDuplicates()
coaches_df_clean = coaches_df_cleaned.dropDuplicates()
entries_gender_df_clean = entries_gender_df.dropDuplicates()
medals_df_clean = medals_df.dropDuplicates()
teams_df_clean = teams_df.dropDuplicates()

# COMMAND ----------

# Check duplicates for each dataframe
def check_duplicates(df, df_name):
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    duplicate_rows = total_rows - distinct_rows
    
    print(f"{df_name}:")
    print(f"  Total Rows: {total_rows}")
    print(f"  Distinct Rows: {distinct_rows}")
    print(f"  Duplicate Rows: {duplicate_rows}")
    print()

# Check all dataframes
check_duplicates(athletes_df_clean, "Athletes")
check_duplicates(coaches_df_clean, "Coaches")
check_duplicates(entries_gender_df_clean, "Entries Gender")
check_duplicates(medals_df_clean, "Medals")
check_duplicates(teams_df_clean, "Teams")

# COMMAND ----------

## Top Countries by Total Medals
from pyspark.sql.functions import col, sum as spark_sum

# Calculate total medals per country
medals_summary = medals_df_clean.groupBy("TeamCountry") \
    .agg(
        spark_sum("Gold").alias("Total_Gold"),
        spark_sum("Silver").alias("Total_Silver"),
        spark_sum("Bronze").alias("Total_Bronze"),
        (spark_sum("Gold") + spark_sum("Silver") + spark_sum("Bronze")).alias("Total_Medals")
    ) \
    .orderBy(col("Total_Medals"), ascending=False)

print("=== TOP 10 COUNTRIES BY TOTAL MEDALS ===")
medals_summary.show(10)

# COMMAND ----------

# Countries with most athletes
athletes_by_country = athletes_df_clean.groupBy("Country") \
    .agg(count("*").alias("Number_of_Athletes")) \
    .orderBy(col("Number_of_Athletes"), ascending=False)

print("=== TOP 15 COUNTRIES BY ATHLETE COUNT ===")
athletes_by_country.show(15)

# COMMAND ----------

from pyspark.sql.functions import col

# Sports with most gender balance/imbalance
gender_analysis = entries_gender_df_clean.withColumn(
    "Total_Participants", col("Female") + col("Male")
).withColumn(
    "Female_Percentage", (col("Female") / (col("Female") + col("Male")) * 100)
).withColumn(
    "Male_Percentage", (col("Male") / (col("Female") + col("Male")) * 100)
)

print("=== GENDER DISTRIBUTION BY DISCIPLINE ===")
gender_analysis.select("Discipline", "Female", "Male", "Female_Percentage", "Male_Percentage") \
    .orderBy(col("Total_Participants"), ascending=False) \
    .show(20, truncate=False)

# COMMAND ----------

# Join athletes and coaches to calculate ratio
athlete_count = athletes_df_clean.groupBy("Country") \
    .agg(count("*").alias("Athlete_Count"))

coach_count = coaches_df_clean.groupBy("Country") \
    .agg(count("*").alias("Coach_Count"))

coach_athlete_ratio = athlete_count.join(coach_count, "Country", "inner") \
    .withColumn("Athlete_per_Coach", col("Athlete_Count") / col("Coach_Count")) \
    .orderBy(col("Athlete_per_Coach"))

print("=== COACH-TO-ATHLETE RATIO BY COUNTRY ===")
coach_athlete_ratio.show(15)

# COMMAND ----------

# Define the output path
storage_account_name = "tokyoolympicdata05"
container_name = "tokyo-olympic-data"
output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/transformed-data/"

print("=== WRITING CLEANED DATA TO TRANSFORMED FOLDER ===\n")

# 1. Write Athletes (single partition CSV)
print("Writing Athletes...")
athletes_df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}athletes")
print("✓ Athletes written successfully")

# 2. Write Coaches (single partition CSV)
print("Writing Coaches...")
coaches_df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}coaches")
print("✓ Coaches written successfully")

# 3. Write Entries Gender (single partition CSV)
print("Writing Entries Gender...")
entries_gender_df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}entries_gender")
print("✓ Entries Gender written successfully")

# 4. Write Medals (single partition CSV)
print("Writing Medals...")
medals_df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}medals")
print("✓ Medals written successfully")

# 5. Write Teams (single partition CSV)
print("Writing Teams...")
teams_df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}teams")
print("✓ Teams written successfully")

print("\n" + "="*60)
print("✓ ALL CLEANED DATA WRITTEN TO TRANSFORMED-DATA FOLDER!")
print("="*60)
