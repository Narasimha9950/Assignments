from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as pf
from pyspark.sql.functions import sort_array
from pyspark.sql.functions import *
from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("").config("spark.dynamicAllocation.enabled", True) \
    .config("spark.executor.cores", 2) \
    .config("spark.dynamicAllocation.minExecutors", 2) \
    .config("spark.dynamicAllocation.maxExecutors", 7) \
    .config("spark.dynamicAllocation.initialExecutors", 2) \
    .config("spark.ui.port", 6051).getOrCreate()


df1=spark.read.parquet("s3://connecto-models/_temp/vdp_views/data/2022/07/01/")
print(df1.show(5))
df2=df1.groupBy("anonymousId").agg(pf.collect_list("used_carid"),pf.collect_list("timestamp"))
df3=df2.withColumnRenamed("collect_list(used_carid)","usedcar").withColumnRenamed("collect_list(timestamp)","time")
df4=df3.select("*",sort_array(df3.time)).withColumnRenamed("sort_array(time, true)","sotime")
print(df4.show(5))

def cor(b,c):
    k=(int(c[11:13])*3600+int(c[14:16])*60+int(c[17:19])) - (int(b[11:13])*3600+int(b[14:16])*60+int(b[17:19]))
    return k

def ses(x):
    lis=[1]
    session=1
    for t in range(len(x)-1):
        if cor(x[t],x[t+1])<1800:
            lis.append(session)
        else:
            session+=1
            lis.append(session)
    return lis  

UDF = udf(lambda z:ses(z),StringType())   
df5=df4.withColumn("sessionnumber", UDF(df4.sotime))
df6=df5.withColumn("sessionnumber",split(col("sessionnumber"),",").cast("array<long>"))
df7=df6.withColumn("sessionnumber",explode(col("sessionnumber")))
df8=df7.select("anonymousId","sessionnumber")
df8=df8.dropna()
df9=df8.groupBy("anonymousId","sessionnumber").count().withColumnRenamed("count","vdp_views")
print(df9.show(10))
print(df9.describe().show())
users=df9.filter(df9.vdp_views>1).count()
print(users)
