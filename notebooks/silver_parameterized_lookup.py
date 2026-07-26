# Databricks Notebook: Parameterized Silver Lookup Load


# Create parameters

dbutils.widgets.text("sourcefolder", "customers")
dbutils.widgets.text("targetfolder", "customers")


# Get parameter values

sourcefolder = dbutils.widgets.get("sourcefolder")
targetfolder = dbutils.widgets.get("targetfolder")


# Define storage paths

storage_account = "operationsperformace"

source_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{sourcefolder}/"
target_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/{targetfolder}/"

print("Source path:", source_path)
print("Target path:", target_path)


# Read CSV data from Bronze

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

display(df)

# COMMAND ----------

# Write data to Silver as Delta

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)

print(f"Silver load completed for: {targetfolder}")
