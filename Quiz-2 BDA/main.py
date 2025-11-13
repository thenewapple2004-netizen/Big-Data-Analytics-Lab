import os
from pyspark.sql import SparkSession, Row
from dotenv import load_dotenv

load_dotenv()


HDFS_HOST = os.getenv("HDFS_HOST", "namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/users")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "HDFS_Parquet_Assignment")
LOCAL_PARQUET_PATH = os.getenv("LOCAL_PARQUET_PATH", "./users.parquet")

# HDFS URI
HDFS_URI = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"
HDFS_FILE = f"{HDFS_BASE_DIR}/users.parquet"


spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .master(SPARK_MASTER_URL) \
    .config("spark.hadoop.fs.defaultFS", HDFS_URI) \
    .getOrCreate()

print("✅ Spark Session started successfully!\n")


data = [
    Row(id=1, name="Ahmad", email="ahmad@example.com"),
    Row(id=2, name="Sara", email="sara@example.com"),
    Row(id=3, name="Ali", email="ali@example.com"),
    Row(id=4, name="Mujeeb", email="mujeeb@example.com")
]

df = spark.createDataFrame(data)
print("📦 Created sample DataFrame:\n")
df.show(truncate=False)


try:

    df.write.mode("overwrite").parquet(HDFS_FILE)
    print(f"💾 Data written to HDFS path: {HDFS_FILE}\n")
    parquet_path = HDFS_FILE
except Exception as e:

    print("⚠️ HDFS not reachable, writing locally instead.")
    df.write.mode("overwrite").parquet(LOCAL_PARQUET_PATH)
    print(f"💾 Data written to local path: {LOCAL_PARQUET_PATH}\n")
    parquet_path = LOCAL_PARQUET_PATH


print("📖 Reading data back...\n")
read_df = spark.read.parquet(parquet_path)
read_df.show(truncate=False)


read_df.createOrReplaceTempView("users")

print("\n🔍 Query 1: Select all users")
spark.sql("SELECT * FROM users").show()

print("\n📧 Query 2: Select only names and emails")
spark.sql("SELECT name, email FROM users").show()

print("\n🔢 Query 3: Count total users")
spark.sql("SELECT COUNT(*) AS total_users FROM users").show()


spark.stop()
print("🏁 Spark job completed successfully!")
