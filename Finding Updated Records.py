# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = ms_employee_salary.groupBy("id", "first_name", "last_name","department_id").agg(f.max("salary").alias("salary")).sort('id')

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
