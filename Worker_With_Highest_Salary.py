# Import your libraries
import pyspark

from pyspark.sql.functions import col, dense_rank, desc
from pyspark.sql.window import Window

# Start writing code

df = worker.join(title, on = (worker.worker_id == title.worker_ref_id), how ='inner')
#df.show()

df = df.withColumn("dnk", dense_rank().over(Window.orderBy(col("salary").desc())))

df = df.filter(col("dnk")==1).select("worker_title")


# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()