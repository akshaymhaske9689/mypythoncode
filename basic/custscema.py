from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("csv without header").getOrCreate()

heading=StructType([StructField("cid",IntegerType(),True),
StructField("cname",StringType(),True),
StructField("city",StringType(),True),
StructField("__corrupt_record",StringType(),True)


])

df=spark.read.format("CSV").option("inferschema","true").schema(heading).option("mode","DROPMALFROMED").option("columnNameOfCorruptRecord","__corrupt_Record"). load("C:/c/cust1.txt")

df.show()