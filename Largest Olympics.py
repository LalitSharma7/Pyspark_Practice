# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = olympics_athletes_events.groupBy("games").agg(
    f.countDistinct("id").alias("athletes_count")).orderBy(f.desc("athletes_count")).limit(1)

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
