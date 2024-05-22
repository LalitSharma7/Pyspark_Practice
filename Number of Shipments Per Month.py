import pyspark.sql.functions as F

amazon_shipment = amazon_shipment.withColumn('year_month', F.date_format(F.col('shipment_date'), 'yyyy-MM'))

result = amazon_shipment.groupby('year_month')\
          .agg(F.count('shipment_id').alias('count'))\
             .orderBy('year_month').toPandas()

result
