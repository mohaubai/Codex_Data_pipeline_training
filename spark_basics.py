from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

spark = SparkSession.builder \
	.appName("CodexDE") \
	.master("local[*]") \
	.getOrCreate()


df = spark.read.csv("orders.csv", header=True, inferSchema=True)

#df.show()
#df.printSchema()

clean_df = df.filter(df.amount > 0).filter(df.order_date.isNotNull())

city_revenue = clean_df.groupby("city").agg({"amount": "sum"})
city_revenue.write.mode("overwrite").parquet("output/city_revenue")

df_back = spark.read.parquet("output/city_revenue")
#df_back.show()

window = Window.partitionBy("city").orderBy(df.amount.desc())
clean_df.withColumn("rank", rank().over(window)).show()

clean_df.write.mode("overwrite").parquet("output/clean_orders")