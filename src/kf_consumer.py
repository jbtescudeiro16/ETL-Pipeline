from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("VehicleStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.output.uri", 
            "mongodb://admin:admin@localhost:27017/vehicles.vehicle_raw?authSource=admin") \
    .getOrCreate()



# Schema das mensagens JSON
schema = StructType([
    StructField("year", IntegerType()),
    StructField("make", StringType()),
    StructField("model", StringType()),
    StructField("trim", StringType()),
    StructField("body", StringType()),
    StructField("transmission", StringType()),
    StructField("state", StringType()),
    StructField("condition", DoubleType()),
    StructField("odometer", DoubleType()),
    StructField("color", StringType()),
    StructField("interior", StringType()),
    StructField("seller", StringType()),
    StructField("mmr", DoubleType()),
    StructField("sellingPrice", DoubleType()),
    StructField("saleDate", StringType())
])

# Ler do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Parse JSON
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
print(parsed)

# Query 1: escrever raw para MongoDB
query_debug = parsed.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

query_debug.awaitTermination()

print("--------------")
print("Streaming to MongoDB...")
print("--------------")
# # Query 2: agregações (ex: count por make em janela de 5 min)
# aggregated = parsed \
#     .withWatermark("saleDate", "10 minutes") \
#     .groupBy(window("saleDate", "5 minutes"), "make") \
#     .agg(count("*").alias("count"), avg("sellingPrice").alias("avg_price"))

# query_agg = aggregated.writeStream \
#     .format("mongodb") \
#     .option("uri", "mongodb://admin:admin@mongodb:27017/vehicles.vehicle_aggregates?authSource=admin") \
#     .option("checkpointLocation", "/tmp/checkpoint_agg") \
#     .outputMode("update") \
#     .start()

# spark.streams.awaitAnyTermination()
