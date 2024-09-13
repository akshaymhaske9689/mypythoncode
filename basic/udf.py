from pyspark.sql import SparkSession
from pyspark.sql.functions import upper,when,lit,udf


spark=SparkSession.builder.appName("csv without header").getOrCreate()

spark.sparkContext.setLogLevel("INFO")
spark.sparkContext.setLogLevel("WARN")
df=spark.read.format("CSV").option("header","true").\
    option("inferschema","true").load("C:\c\cust3.txt")
def negativetozero(n):
    if n <= 0:
        return 0
    else:
        return n

negativetozero=udf(negativetozero)

df1=df.withColumn("temp",negativetozero(df['temp']))

df1.show()