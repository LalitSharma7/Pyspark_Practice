# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = fact_events.groupBy("client_id", f.month("time_id")).agg(f.countDistinct("user_id"))



# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
