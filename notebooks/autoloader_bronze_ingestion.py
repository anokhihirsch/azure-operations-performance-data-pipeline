# Databricks Notebook: AutoLoader Bronze Ingestion (incremental data loading)
# This notebook is designed for Azure Databricks.
# It uses Auto Loader to incrementally ingest CSV files from ADLS Gen2 into a Bronze Delta table.
# SQL
CREATE SCHEMA operations_catalog.net_schema;

# defining variable for checkpoint/schema location
checkpoint_location = "abfss://bronze@operationsperformace.dfs.core.windows.net/checkpoint/orders"

# reading new CSV files using Auto Loader
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_location)
    .option("header", "true")
    .load("abfss://bronze@operationsperformace.dfs.core.windows.net/orders/")
)

# streaming preview
display(df)

# writing data into bronze
(
    df.writeStream
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime="10 seconds") 
    .start("abfss://bronze@operationsperformace.dfs.core.windows.net/bronze_orders/")
)

# this part   "  .trigger(processingTime="10 seconds") " will keep running Every 10 seconds, check for new files. 
# Use ".trigger(availableNow=True)" for Process all files currently available, then stop.
