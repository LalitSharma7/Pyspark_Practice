# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
f = spotify_worldwide_daily_song_ranking.groupBy("artist")\
.agg(f.count('*').alias("n_occurences")).orderBy(f.desc("n_occurences"))


# To validate your solution, convert your final pySpark df to a pandas df
f.toPandas()
