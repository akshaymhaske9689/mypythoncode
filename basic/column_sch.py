
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("csv without header").getOrCreate()

heading=StructType([StructField("cid",IntegerType(),True),
StructField("cname",StringType(),True),
StructField("sal",StringType(),True),
StructField("__corrupt_Record",StringType(),True)
])

df=spark.read.format("CSV").option("inferschema","true").\
    schema(heading).option("mode","PERMISSIVE").\
    option("columnNameOfCorruptRecord","__corrupt_Record").\
    load("C:\c\emp1.txt")

df.show()
df.printSchema()
