
# Databricks Notebook: Lookup Silver Files
# This notebook creates a list of lookup/reference files
# that will be passed dynamically to the Silver notebook.

files = [
    {
        "sourcefolder": "customers",
        "targetfolder": "customers"
    },
    {
        "sourcefolder": "products",
        "targetfolder": "products"
    },
    {
        "sourcefolder": "sellers",
        "targetfolder": "sellers"
    }
]

dbutils.jobs.taskValues.set(
    key="silver_lookup_files",
    value=files
)
