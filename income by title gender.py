# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df_bonus = sf_bonus.groupBy("worker_ref_id").agg(f.sum("bonus").alias("bonus"))

df = df_bonus.join(sf_employee, df_bonus.worker_ref_id==sf_employee.id, how="inner").withColumn("total_salary", f.col("salary")+f.col("bonus")).select("employee_title", "sex", "total_salary")

df = df.groupBy("employee_title", "sex").agg(f.avg("total_salary").alias("avg_total_comp"))


# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
