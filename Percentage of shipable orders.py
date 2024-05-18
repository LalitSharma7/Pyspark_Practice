# Import your libraries
import pyspark
from pyspark.sql import functions as f
# Start writing code
df = orders.join(customers, orders.cust_id == customers.id, how = 'inner').select(orders.id, customers.address)

df = df.select(
    (f.count(f.when(f.col("address").isNotNull(), 1)) / f.count("*"))*100
)

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
