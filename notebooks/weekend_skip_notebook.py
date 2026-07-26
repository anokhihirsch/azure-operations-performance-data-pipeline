# Databricks Notebook: Weekend Skip Notebook

weekday_value = dbutils.jobs.taskValues.get(
    taskKey="WeekdayLookup",
    key="weekoutput"
)

print(f"Pipeline skipped. Today is weekday number: {weekday_value}")
print("Main Silver transformation does not run on this condition.")
