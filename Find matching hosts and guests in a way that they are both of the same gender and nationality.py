# Import your libraries
import pyspark
from pyspark.sql import functions as f

# Start writing code
df = airbnb_hosts.join(airbnb_guests, (airbnb_hosts.gender==airbnb_guests.gender) & (airbnb_hosts.nationality==airbnb_guests.nationality), how="inner")
     .distinct()
     .select(airbnb_hosts.host_id, airbnb_guests.guest_id)

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
