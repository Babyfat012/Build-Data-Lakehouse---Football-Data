from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Silver Layer Processing")
    .master("spark://spark-master:7077") # spark chạy ở đâu
    .getOrCreate()
)

print(f"Spark version: {spark.version}")

# Test kết nối ghi/đọc S3A 
test_path = "s3a://lakehouse/tmp/s3a_test/"
spark.range(5).withColumn("x2", F.col("id")* 2).write.mode("overwrite").parquet(test_path)
print("Write test data to ", test_path)
spark.read.parquet(test_path).show()