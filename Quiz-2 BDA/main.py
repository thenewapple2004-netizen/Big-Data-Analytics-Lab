import os
from pyspark.sql import SparkSession, Row
from dotenv import load_dotenv

# ------------------ Load environment variables ------------------
load_dotenv()

HDFS_HOST = os.getenv("HDFS_HOST", "hdfs-namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/users")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "HDFS_Parquet_Assignment")

# Full HDFS path
HDFS_URI = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"
HDFS_FILE = f"{HDFS_BASE_DIR}/users.parquet"

# Set Java/Hadoop paths for Windows if not globally set
os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME", "C:\\Program Files\\Java\\jdk-11")
os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")

# ------------------ Create Spark Session ------------------
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .master(SPARK_MASTER_URL) \
    .config("spark.hadoop.fs.defaultFS", HDFS_URI) \
    .getOrCreate()

print("✅ Spark Session started successfully!\n")

# ------------------ 1️⃣ Write Sample Data to HDFS (Parquet) ------------------
data = [
    Row(id=1, name="Ahmad", email="ahmad@example.com"),
    Row(id=2, name="Sara", email="sara@example.com"),
    Row(id=3, name="Ali", email="ali@example.com"),
    Row(id=4, name="Waleed", email="waleed@example.com")
]

df = spark.createDataFrame(data)
print("📦 Writing DataFrame to HDFS as Parquet...\n")
df.write.mode("overwrite").parquet(HDFS_FILE)
print(f"✅ Data written to HDFS path: {HDFS_FILE}\n")

# ------------------ 2️⃣ Read Data from HDFS (Parquet) ------------------
print("📖 Reading data back from HDFS Parquet...\n")
read_df = spark.read.parquet(HDFS_FILE)
read_df.show(truncate=False)

# ------------------ 3️⃣ Apply SparkSQL Queries ------------------
read_df.createOrReplaceTempView("users")

print("\n🔍 Query 1: Select all users")
spark.sql("SELECT * FROM users").show()

print("\n📧 Query 2: Select only names and emails")
spark.sql("SELECT name, email FROM users").show()

print("\n🔢 Query 3: Count total users")
spark.sql("SELECT COUNT(*) AS total_users FROM users").show()

# ------------------ Stop Spark ------------------
spark.stop()
print("🏁 Spark job completed successfully!")
