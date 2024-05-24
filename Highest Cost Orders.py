# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = customers.join(orders, customers.id==orders.cust_id, how = 'inner').filter(
    (orders.order_date>='2019-02-01') & (orders.order_date<='2019-05-01'))

df = df.groupBy("first_name", "order_date").agg(f.sum("total_order_cost").alias("max_cost")).orderBy(f.desc("max_cost")).limit(1)

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
