import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = airbnb_contacts.groupby('id_guest').agg(F.sum('n_messages').alias('n_messages')).orderBy(F.desc('n_messages'), 'id_guest')
df = df.withColumn('ranking', F.dense_rank().over(Window.orderBy(F.desc('n_messages'))))
df.toPandas()
