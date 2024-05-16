# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code
salary_diff = (
    db_employee
    .join(db_dept, db_employee['department_id']==db_dept['id'])
    .agg(
        F.max(F.when(F.col('department')=='engineering', F.col('salary'))).alias('eng_max'),
        F.max(F.when(F.col('department')=='marketing', F.col('salary'))).alias('mar_max')
    )
    .withColumn('salary_difference', F.abs(F.col('eng_max') - F.col('mar_max')))
    .select('salary_difference')
)

# To validate your solution, convert your final pySpark df to a pandas df
salary_diff.toPandas()
