import pyspark.sql.functions as F

result = sf_restaurant_health_violations.select('business_name') \
    .withColumn('business_type', F.when(F.lower(F.col('business_name')).like('%school%'), 'school') \
        .when(F.lower(F.col('business_name')).like('%restaurant%'), 'restaurant') \
        .when(F.lower(F.col('business_name')).like('%cafe%') | F.lower(F.col('business_name')).like('%coffee%') | F.lower(F.col('business_name')).like('%café%'), 'cafe') \
        .otherwise('other')) \
    .select('business_name', 'business_type') \
    .dropDuplicates()

result.toPandas()
