from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.window import Window



spark=SparkSession.builder.appName("SCDType2").getOrCreate()



#initial data

df_ini=spark.read.format("CSV").option("header","true").option("inferschema","true").load("C:\c\cust_ini.csv")



df_ini=df_ini.withColumn("start_date",date_add(current_date(),-1)).withColumn("end_date",lit('')).withColumn("current_flag",lit("Y"))



#updated data

df_upd=spark.read.format("CSV").option("header","true").option("inferschema","true").load("C:\c\cust_upd.csv")



df_upd=df_upd.withColumn("start_date",current_date()).withColumn("end_date",lit('')).withColumn("current_flag",lit("Y"))





df=df_ini.union(df_upd)



df=df.withColumn("rn",row_number().over(Window.partitionBy("cid").orderBy(desc("start_date"))))



df = df.withColumn("current_flag",when(df["rn"] == 1, lit("Y")).otherwise(lit("N")))



df= df.withColumn("end_date",lag("start_date",1,'').over(Window.partitionBy("cid").orderBy(desc("start_date"))))



df=df.drop("rn")



df.show()