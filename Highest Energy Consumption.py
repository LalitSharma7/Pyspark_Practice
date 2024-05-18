# Import your libraries
import pyspark
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# Start writing code
df = fb_eu_energy.union(fb_asia_energy).union(fb_na_energy)

df = df.groupBy("date").agg(f.sum("consumption").alias("consumption")).orderBy(f.desc("consumption"))
window = Window.orderBy(f.desc("consumption"))

df = df.withColumn("rn", f.dense_rank().over(window))
df = df.filter(f.col("rn")==1).select(f.col("date"), f.col("consumption"))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
