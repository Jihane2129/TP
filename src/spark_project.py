from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("FlightDelayAnalysis").getOrCreate()

# Task 1: Data Loading and Exploration
flight_data = spark.read.csv("../Datasets/flights.csv", header=True, inferSchema=True)
flight_data.show(10)
flight_data.printSchema()

# Task 2: Data Cleansing with DataFrame API
flight_data = flight_data.withColumn("delayed", F.when(F.col("arrival_delay") > 15, 1).otherwise(0))
flight_data.show(10)

# Identifier les colonnes critiques pour votre analyse
critical_columns = ["AIRLINE", "DEPARTURE_DELAY", "CANCELLATION_REASON", "ORIGIN_AIRPORT", "ARRIVAL_DELAY", "DESTINATION_AIRPORT", "DELAYED"]
flight_data = flight_data.na.fill(0, subset=critical_columns)
flight_data.show(10)

# Task 3: Data Aggregation and Grouping

average_delay_by_airline = flight_data.groupBy("AIRLINE").agg(F.avg("ARRIVAL_DELAY").alias("average_delay_by_airline"))
average_delay_by_origin = flight_data.groupBy("ORIGIN_AIRPORT").agg(F.avg("ARRIVAL_DELAY").alias("average_delay_by_origin"))

# Afficher les résultats
print("Average Delay by Airline:")
average_delay_by_airline.show()

print("Average Delay by Origin Airport:")
average_delay_by_origin.show()

# Task 4: Sorting and Ordering
top_10_delayed_flights = flight_data.orderBy("ARRIVAL_DELAY", ascending=False).limit(10)
print("Top 10 Most Delayed Flights:")
top_10_delayed_flights.show()

# Task 5: Advanced Data Manipulation with Window Functions
window_spec = Window.partitionBy("ORIGIN_AIRPORT").orderBy(F.desc("DEPARTURE_DELAY"))
ranked_airports = flight_data.withColumn("rank", F.rank().over(window_spec))
print("Ranking Airports by Number of Departure Flights:")
ranked_airports.show()

# Task 6: RDD Operations
flight_data_rdd = flight_data.rdd

# Perform a map-reduce operation to count the number of flights per airline
flights_per_airline = flight_data_rdd.map(lambda row: (row['AIRLINE'], 1)).reduceByKey(lambda a, b: a + b)

# Display the result
flights_per_airline.collect()


# Task 7: Partitioning
# Use the partitionBy operation to partition the data based on a suitable key, such as destination airport
partitioned_data = flight_data_rdd.map(lambda row: (row['DESTINATION_AIRPORT'], row)).partitionBy(8)

# Display the result
partitioned_data.glom().collect()
'''''
# Task 8: Analysis and Reporting
# Your insights and findings can be added here based on the results obtained from the above operations.
'''
# Stop the Spark session
spark.stop()
