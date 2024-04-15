# Import your libraries
import pyspark

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, concat, countDistinct, dense_rank, desc

# Start writing code

df_new = user_flags.join(flag_review, on = (user_flags.flag_id==flag_review.flag_id), how = 'inner')
df_new = df_new.filter(col("reviewed_outcome")=="APPROVED")
df_new = df_new.withColumn("username", concat(col("user_firstname"), lit(" "), col("user_lastname")))
df_new = df_new.groupby(col("username")).agg(countDistinct(col("video_id")).alias("count_of_Id"))
df_new = df_new.withColumn("rank", dense_rank().over(Window.orderBy(col("count_of_id").desc())))
df_new = df_new.filter(col("rank")==1)
df_new = df_new.select("username")


#df_new.show()

# To validate your solution, convert your final pySpark df to a pandas df
df_new.toPandas()