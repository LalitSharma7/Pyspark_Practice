import pyspark.sql.functions as F

result = los_angeles_restaurant_health_inspections.filter((F.col('facility_name') == 'STREET CHURROS') & (F.col('score') < 95)).select('activity_date', 'pe_description')
#result = result.withColumn('activity_date', F.date_format('activity_date', 'yyyy-MM-dd'))
result.toPandas()
