# Import your libraries
import pyspark
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# Start writing code
df = online_orders.withColumn("revenue", f.col("cost_in_dollars")*f.col("units_sold"))\
    .filter((f.month(online_orders["date"]) >= 1) & (f.month(online_orders["date"]) <= 6))\
        .groupBy("product_id")\
            .agg(f.sum("revenue").alias("total"))

window = Window.orderBy(f.desc("total"))

df = df.withColumn("rn", f.dense_rank().over(window))
df = df.filter(df.rn<=5).select("product_id", "total")


# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
