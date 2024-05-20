# Import your libraries
import pyspark
from pyspark.sql import functions as f
# Start writing code
df = playbook_events.join(playbook_users, playbook_events.user_id==playbook_users.user_id, how="inner" ).select(playbook_events.user_id, "device", "language")

df = df.groupBy("language").agg(
    f.countDistinct(
        f.when(
            f.col("device").isin('macbook pro', 'iphone 5s', 'ipad air'), f.col("user_id")
            )
        ).alias("n_apple_users"), 
        f.countDistinct("user_id").alias("n_total_users")
    )
    

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
