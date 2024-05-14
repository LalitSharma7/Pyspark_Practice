# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = worker.withColumn("month", f.month("joining_date"))
df = df.filter((df.month>=4) & (df.department=="Admin")).count()
df



# To validate your solution, convert your final pySpark df to a pandas df
