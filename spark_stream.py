from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("address", StringType()),
    StructField("post_code", StringType()),
    StructField("email", StringType()),
    StructField("username", StringType()),
    StructField("registered_date", StringType()),
    StructField("phone", StringType()),
    StructField("picture", StringType())
])

spark = SparkSession.builder \
    .appName("UsersKafkaToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "users_data") \
    .option("startingOffsets", "earliest") \
    .load()

users_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("user")
).select("user.*").filter(col("id").isNotNull())

def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="spark_streams", table="created_users") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} written to Cassandra")

query = users_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .option("checkpointLocation", "/tmp/users_checkpoint") \
    .start()

query.awaitTermination()
