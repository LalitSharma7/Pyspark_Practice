# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = facebook_employees.join(facebook_hack_survey, facebook_employees.id == facebook_hack_survey.employee_id, "inner")\
     .select(facebook_employees.location, facebook_hack_survey.popularity)
df = df.groupBy("location").agg(f.avg("popularity").alias("popularity"))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
