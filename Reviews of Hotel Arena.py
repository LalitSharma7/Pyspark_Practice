# Import your libraries
import pyspark

from pyspark.sql import functions as f

# Start writing code
df = hotel_reviews.filter(hotel_reviews.hotel_name == "Hotel Arena")
df = df.groupBy("reviewer_score", "hotel_name").agg(f.count("reviewer_score").alias("n_reviews"))


# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
