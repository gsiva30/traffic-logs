# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DailyWebTrafficAnalysis").getOrCreate()


# Define variables
access_key = dbutils.secrets.get(scope="traffic_scope", key="Acess_key")
bucket_name = "databrickspractise"
mount_point = "/mnt/traffic_logs"
secret_key = dbutils.secrets.get(scope="traffic_scope", key="Secret_key")
# Construct the S3 URL
s3_url = f"s3a://{access_key}:{secret_key}@{bucket_name}"
# Mount the S3 bucket

dbutils.fs.mount(
    source=s3_url,
    mount_point=mount_point
)

web_traffic_df = spark.read.csv("dbfs:/mnt/traffic_logs/web_traffic_logs.csv", header=True, inferSchema=True)

# Display the data
web_traffic_df.show()



# COMMAND ----------

clean_df = web_traffic_df.filter(web_traffic_df.page_url.isNotNull() & web_traffic_df.visit_date.isNotNull())

clean_df.show()
clean_df.count()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, avg

# Define a window for daily traffic
daily_window = Window.partitionBy("page_url").orderBy("visit_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate daily traffic
daily_traffic_df = clean_df.withColumn("daily_views", sum("views").over(daily_window))

# Define a window for the 7-day moving average
moving_avg_window = Window.partitionBy("page_url").orderBy("visit_date").rowsBetween(-6, 0)

# Calculate 7-day moving average
moving_avg_df = daily_traffic_df.withColumn("7_day_moving_avg", avg("daily_views").over(moving_avg_window))


# COMMAND ----------

from pyspark.sql.functions import rank

# Define a window for ranking
rank_window = Window.partitionBy("visit_date").orderBy(col("daily_views").desc())

# Rank pages and filter the top 5
ranked_df = moving_avg_df.withColumn("rank", rank().over(rank_window))
top_pages_df = ranked_df.filter(col("rank") <= 5)


top_pages_df.show()




# COMMAND ----------

top_pages_df.write.format("delta").mode("overwrite").saveAsTable("traffic_logs")



# COMMAND ----------

# write the data to s3 bucket again
top_pages_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/mnt/traffic_logs/traffic_logs_clean")
