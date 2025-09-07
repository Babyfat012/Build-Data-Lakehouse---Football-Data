from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Silver Layer Processing")
    .master("spark://spark-master:7077") # spark chạy ở đâu
    .getOrCreate()
)

print(f"Spark version: {spark.version}")

# Test kết nối ghi/đọc S3A 
# test_path = "s3a://lakehouse/tmp/s3a_test/"
# spark.range(5).withColumn("x2", F.col("id")* 2).write.mode("overwrite").parquet(test_path)
# print("Write test data to ", test_path)
# spark.read.parquet(test_path).show()


schema = T.StructType([
    T.StructField("game_id", T.LongType()),
    T.StructField("club_id", T.LongType()),
    T.StructField("own_goals", T.IntegerType()),
    T.StructField("own_position", T.IntegerType()),
    T.StructField("own_manager_name", T.StringType()),
    T.StructField("opponent_id", T.LongType()),
    T.StructField("opponent_goals", T.IntegerType()),
    T.StructField("opponent_position", T.IntegerType()),
    T.StructField("opponent_manager_name", T.StringType()),
    T.StructField("hosting", T.StringType()),
    T.StructField("is_win", T.StringType())
])

bronze = "s3a://lakehouse/bronze/csv/club_games.csv"
silver = "s3a://lakehouse/silver/club_games"

df = (spark.read
        .option("header", "true")
        .schema(schema)
        .csv(bronze)
    )
    
# Xử lý dữ liệu
df = df.withColumn("hosting", F.trim(F.col("hosting")))

df = df.withColumn("is_win", F.col("is_win").cast("int"))

df = (df
        .withColumn("goal_diff", F.col("own_goals") - F.col("opponent_goals"))
        .withColumn("is_home", F.when(F.col("hosting") == F.lit("home"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_draw", F.when(F.col("goal_diff") == 0, 1).otherwise(0))
        .withColumn("is_loss", F.when(F.col("goal_diff") < 0, 1).otherwise(0))
        .withColumn("is_win_check", F.when(F.col("goal_diff") > 0, 1).otherwise(0))
        .withColumn("label_consistent", F.col("is_win") == F.col("is_win_check"))
    )


df = (df.drop("own_position", "own_manager_name",
               "opponent_position", "opponent_manager_name")
               .dropDuplicates(["game_id", "club_id", "opponent_id"])
     )

df.write.mode("overwrite").parquet(silver)
spark.stop()