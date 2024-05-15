# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = forbes_global_2010_2014.orderBy(f.desc("profits")).limit(3).select("company", "profits")

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
