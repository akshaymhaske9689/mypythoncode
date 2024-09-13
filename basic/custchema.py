from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("csv without header").getOrCreate()

# Define schema
heading = StructType([
    StructField("cid", IntegerType(), True),
    StructField("cname", StringType(), True)
      # Corrected 'Structfield' to 'StructField'
])

# Read the CSV file with the defined schema and no header
df = spark.read.format("csv") \
    .option("header", "false") \
    .schema(heading) \
    .load(r"C:\c\cust.txt")  # Use raw string or double backslashes for Windows path

# Show the DataFrame
df.show()
