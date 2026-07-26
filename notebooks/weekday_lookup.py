# Databricks Notebook: Weekday Lookup

# create parameter
dbutils.widgets.text("weekday", "7")

# get parameter value
weekday = int(dbutils.widgets.get("weekday"))

# return value to Databricks Workflow
dbutils.jobs.taskValues.set(
    key="weekoutput",
    value=weekday
)
