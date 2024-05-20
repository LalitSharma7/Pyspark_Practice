import pyspark.sql.functions as F

result = car_launches \
    .groupBy('company_name') \
    .pivot('year') \
    .agg(F.count('*')) \
    .withColumn('net_products', F.col('2020') - F.col('2019')) \
    .select('company_name', 'net_products') \
    .toPandas()
