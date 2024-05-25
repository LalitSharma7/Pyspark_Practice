# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = sf_restaurant_health_violations\
      .filter(f.col("business_name")=='Roxanne Cafe')\
          .withColumn("year", f.year("inspection_date"))

df = df.groupBy("year")\
        .agg(f.count("violation_id").alias("violation_count"))\
           .orderBy("year")
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
