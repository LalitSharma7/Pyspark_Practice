import pyspark

from pyspark.sql import functions as f

df = airbnb_hosts.join(airbnb_units, airbnb_hosts.host_id==airbnb_units.host_id, how="inner" ).filter((airbnb_hosts.age<30) & (airbnb_units.unit_type == 'Apartment' ))
df = df.groupBy("nationality").agg(f.countDistinct("unit_id").alias("apartment_count"))
df.toPandas()
