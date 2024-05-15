import pyspark.sql.functions as F
from pyspark.sql.window import Window

result = google_gmail_emails.groupby('from_user').agg(F.count('*').alias('total_emails'))
result = result.withColumn('rank', F.row_number().over(Window.orderBy(F.desc('total_emails'), 'from_user')))
result = result.orderBy(F.desc('total_emails'), 'from_user')

result.toPandas()
