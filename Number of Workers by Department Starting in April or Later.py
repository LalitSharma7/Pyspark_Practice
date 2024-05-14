# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
#worker.show()
df = worker.withColumn("month", f.month("joining_date"))\
    .filter(f.col("month")>=4)\
    .groupBy("department").agg(f.count("worker_id").alias("num_workers"))

df = df.orderBy(f.desc("num_workers"))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
