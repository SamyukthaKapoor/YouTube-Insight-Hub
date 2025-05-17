import sys
import json
import subprocess
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, when, lit, round
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'landing_zone_bucket',
    'landing_zone_path',
    'csv_file',
    'json_file',
    'transformed_zone_bucket',
    'transformed_zone_path'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("YouTube ETL Job Started")

# Define S3 paths
landing_bucket = args['landing_zone_bucket']
landing_path = args['landing_zone_path']
csv_file = args['csv_file']
json_file = args['json_file']
transform_bucket = args['transformed_zone_bucket']
transform_path = args['transformed_zone_path']

print(f"Processing CSV file: {csv_file}")
print(f"Processing JSON file: {json_file}")

# Read CSV data
csv_path = f"s3://{landing_bucket}/{landing_path}{csv_file}"
df_videos = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)

print(f"CSV data loaded with {df_videos.count()} rows")
print("CSV Schema:")
df_videos.printSchema()

# Read JSON data
json_path = f"s3://{landing_bucket}/{landing_path}{json_file}"
raw_json = spark.sparkContext.textFile(json_path).collect()
json_content = ''.join(raw_json)
json_data = json.loads(json_content)

print("JSON data loaded")

# Create DataFrame from JSON category data
try:
    category_items = json_data['items']
    categories = [(item['id'], item['snippet']['title']) for item in category_items]
    df_categories = spark.createDataFrame(categories, ["category_id", "category_name"])
    
    print(f"Categories extracted: {df_categories.count()}")
    print("Categories schema:")
    df_categories.printSchema()
except Exception as e:
    print(f"Error extracting categories: {str(e)}")
    raise

# Clean and transform the data
print("Starting data transformation")

# Handle date fields
df_transformed = df_videos.withColumn(
    "trending_date", 
    F.to_date(F.concat(
        F.lit("20"), 
        F.substring(col("trending_date"), 1, 2), 
        F.lit("-"),
        F.substring(col("trending_date"), 6, 2), 
        F.lit("-"),
        F.substring(col("trending_date"), 4, 2)
    ), "yyyy-MM-dd")
)

df_transformed = df_transformed.withColumn(
    "publish_time", 
    F.to_date(F.substring(col("publish_time"), 1, 10), "yyyy-MM-dd")
)

# Calculate days to trend
df_transformed = df_transformed.withColumn(
    "days_to_trend", 
    F.datediff(col("trending_date"), col("publish_time"))
)

# Convert string columns to appropriate types
numeric_columns = ["views", "likes", "dislikes", "comment_count"]
for column in numeric_columns:
    df_transformed = df_transformed.withColumn(column, col(column).cast(IntegerType()))

# Convert category_id to string for joining
df_transformed = df_transformed.withColumn("category_id", col("category_id").cast("string"))
df_categories = df_categories.withColumn("category_id", col("category_id").cast("string"))

# Join with category data
df_final = df_transformed.join(df_categories, on="category_id", how="left")

print(f"After joins: {df_final.count()} rows")

# Calculate engagement metrics
def calculate_engagement_rate(likes, dislikes, comment_count, views):
    """Calculate engagement rate as percentage of viewers who engaged"""
    if views == 0:
        return 0
    return ((likes + dislikes + comment_count) / views) * 100

def calculate_like_dislike_ratio(likes, dislikes):
    """Calculate ratio of likes to dislikes"""
    if dislikes == 0:
        return likes  # If no dislikes, return likes
    return likes / dislikes

# Register UDFs
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

engagement_rate_udf = udf(calculate_engagement_rate, FloatType())
like_dislike_ratio_udf = udf(calculate_like_dislike_ratio, FloatType())

# Apply UDFs to create new columns
df_final = df_final.withColumn(
    "engagement_rate", 
    F.round(engagement_rate_udf(
        col("likes"), col("dislikes"), col("comment_count"), col("views")
    ), 4)
)

df_final = df_final.withColumn(
    "like_dislike_ratio", 
    F.round(like_dislike_ratio_udf(col("likes"), col("dislikes")), 4)
)

# Categorize videos based on popularity
df_final = df_final.withColumn(
    "popularity_level",
    F.when(col("views") >= 1000000, "High")
    .when(col("views") >= 100000, "Medium")
    .otherwise("Low")
)

print("Transformations completed")
print(f"Final dataframe: {df_final.count()} rows")

# Select and order columns for final output
selected_columns = [
    "video_id", "title", "publish_time", "trending_date", "channel_title", 
    "category_id", "category_name", "tags", "views", "likes", "dislikes", 
    "comment_count", "thumbnail_link", "comments_disabled", "ratings_disabled", 
    "video_error_or_removed", "description", "days_to_trend", 
    "engagement_rate", "like_dislike_ratio", "popularity_level"
]

df_output = df_final.select(selected_columns)

# Write transformed data to S3
output_path = f"s3://{transform_bucket}/{transform_path}youtube_data.csv"
print(f"Writing data to {output_path}")

df_output.write.mode("overwrite").option("header", "true").csv(output_path)

print("ETL Job completed successfully")

# End the job
job.commit()
