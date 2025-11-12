import os
from uuid import uuid4
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when
from dotenv import load_dotenv

load_dotenv()

HDFS_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/users")
HDFS_USERS_PARQUET = f"hdfs://{HDFS_HOST}:9000{HDFS_BASE_DIR}/users.parquet"

# Define schema for users table
USER_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False)
])

def create_spark_session():
    """Create and return a SparkSession configured for HDFS"""
    print(f"\n🔗 Creating SparkSession and connecting to HDFS at hdfs://{HDFS_HOST}:9000 ...")
    try:
        spark = SparkSession.builder \
            .appName("HDFS_Parquet_CRUD") \
            .config("spark.master", "local[*]") \
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_HOST}:9000") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .getOrCreate()
        
        # Test HDFS connection by creating base directory if needed
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(f"hdfs://{HDFS_HOST}:9000"),
            hadoop_conf
        )
        
        # Create base directory if it doesn't exist
        base_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(HDFS_BASE_DIR)
        if not fs.exists(base_path):
            fs.mkdirs(base_path)
            print(f"✅ Created HDFS directory: {HDFS_BASE_DIR}")
        
        print("✅ SUCCESS: SparkSession created and connected to HDFS!")
        return spark
    except Exception as e:
        print(f"❌ ERROR: Spark/HDFS connection failed:\n{e}")
        print("\n💡 Troubleshooting:")
        print("1️⃣ docker compose up -d")
        print("2️⃣ Wait for all services to start (namenode, datanode, spark-master, spark-worker)")
        print("3️⃣ Check HDFS: http://localhost:9871")
        print("4️⃣ Check Spark: http://localhost:8080")
        print("5️⃣ docker ps / docker logs <container_name>")
        exit(1)

def _load_users_df(spark):
    """Load users DataFrame from Parquet file on HDFS"""
    try:
        df = spark.read.parquet(HDFS_USERS_PARQUET)
        return df
    except Exception as e:
        # File doesn't exist, return empty DataFrame
        print(f"ℹ️  Parquet file not found, creating new one...")
        return spark.createDataFrame([], schema=USER_SCHEMA)

def _save_users_df(df):
    """Save users DataFrame to Parquet file on HDFS"""
    df.write.mode("overwrite").parquet(HDFS_USERS_PARQUET)
    print(f"✅ Data saved to {HDFS_USERS_PARQUET}")

def create_user(spark, name, email):
    """Create a new user using Spark SQL"""
    df = _load_users_df(spark)
    
    # Check if user already exists using Spark SQL
    df.createOrReplaceTempView("users")
    existing = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM users 
        WHERE name = '{name}'
    """).collect()[0]['count']
    
    if existing > 0:
        print("⚠️ User with that name already exists.")
        return
    
    # Create new user
    new_user = spark.createDataFrame(
        [(str(uuid4()), name, email)],
        schema=USER_SCHEMA
    )
    
    # Union with existing users and save
    updated_df = df.union(new_user)
    _save_users_df(updated_df)
    
    user_id = new_user.select("id").collect()[0]['id']
    print(f"✅ Created user with ID: {user_id}")

def read_users(spark):
    """Read all users using Spark SQL"""
    df = _load_users_df(spark)
    df.createOrReplaceTempView("users")
    
    result = spark.sql("""
        SELECT id, name, email 
        FROM users 
        ORDER BY name
    """)
    
    users = result.collect()
    print("\n📋 All Users:")
    if not users:
        print("(no users found)")
        return
    
    for user in users:
        print(f"ID: {user['id']} | Name: {user['name']} | Email: {user['email']}")
    
    print(f"\n📊 Total users: {len(users)}")

def update_user(spark, name, new_email):
    """Update user email using Spark SQL"""
    df = _load_users_df(spark)
    df.createOrReplaceTempView("users")
    
    # Check if user exists
    existing = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM users 
        WHERE name = '{name}'
    """).collect()[0]['count']
    
    if existing == 0:
        print("⚠️ No user found.")
        return
    
    # Update using Spark SQL with CASE WHEN
    updated_df = df.withColumn(
        "email",
        when(col("name") == name, new_email).otherwise(col("email"))
    )
    
    _save_users_df(updated_df)
    print(f"✅ Updated email for '{name}' to '{new_email}'")

def delete_user(spark, name):
    """Delete user using Spark SQL"""
    df = _load_users_df(spark)
    df.createOrReplaceTempView("users")
    
    # Check if user exists
    existing = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM users 
        WHERE name = '{name}'
    """).collect()[0]['count']
    
    if existing == 0:
        print("⚠️ No user found.")
        return
    
    # Delete using Spark SQL filter
    updated_df = df.filter(col("name") != name)
    _save_users_df(updated_df)
    print(f"✅ Deleted user '{name}'.")

def search_user(spark, name):
    """Search user by name using Spark SQL"""
    df = _load_users_df(spark)
    df.createOrReplaceTempView("users")
    
    result = spark.sql(f"""
        SELECT id, name, email 
        FROM users 
        WHERE name = '{name}'
    """)
    
    users = result.collect()
    if not users:
        print("⚠️ No user found.")
        return
    
    user = users[0]
    print("\n✅ Found:")
    print(f"ID: {user['id']}")
    print(f"Name: {user['name']}")
    print(f"Email: {user['email']}")

def query_users_spark_sql(spark):
    """Demonstrate additional Spark SQL queries"""
    df = _load_users_df(spark)
    df.createOrReplaceTempView("users")
    
    print("\n📊 Spark SQL Analytics:")
    print("=" * 50)
    
    # Count total users
    total = spark.sql("SELECT COUNT(*) as total FROM users").collect()[0]['total']
    print(f"Total Users: {total}")
    
    # Count users by email domain
    print("\nUsers by Email Domain:")
    domain_count = spark.sql("""
        SELECT 
            SUBSTRING_INDEX(email, '@', -1) as domain,
            COUNT(*) as count
        FROM users
        GROUP BY domain
        ORDER BY count DESC
    """)
    domain_count.show(truncate=False)
    
    # Show sample data
    print("\nSample Data (first 5 users):")
    spark.sql("SELECT * FROM users LIMIT 5").show(truncate=False)

def display_menu():
    print("\n" + "=" * 50)
    print("     HDFS PARQUET + SPARK SQL CRUD OPERATIONS")
    print("=" * 50)
    print("1. Create User")
    print("2. Read All Users")
    print("3. Update User Email")
    print("4. Delete User")
    print("5. Search User by Name")
    print("6. Spark SQL Analytics")
    print("7. Exit")
    print("=" * 50)

def get_user_input(prompt):
    try:
        return input(prompt).strip()
    except KeyboardInterrupt:
        print("\nExiting...")
        exit()

def main():
    print("\n🚀 Starting HDFS Parquet + Spark SQL CRUD Operations...\n")
    spark = create_spark_session()
    
    try:
        while True:
            display_menu()
            choice = get_user_input("\nEnter your choice (1-7): ")
            
            if choice == "1":
                name = get_user_input("Enter user name: ")
                email = get_user_input("Enter user email: ")
                if name and email:
                    create_user(spark, name, email)
                else:
                    print("⚠️ Name and email required.")
            
            elif choice == "2":
                read_users(spark)
            
            elif choice == "3":
                name = get_user_input("User name to update: ")
                new_email = get_user_input("New email: ")
                if name and new_email:
                    update_user(spark, name, new_email)
                else:
                    print("⚠️ Name and new email required.")
            
            elif choice == "4":
                name = get_user_input("User name to delete: ")
                confirm = get_user_input(f"Delete '{name}'? (y/N): ")
                if confirm.lower() in ["y", "yes"]:
                    delete_user(spark, name)
                else:
                    print("Cancelled.")
            
            elif choice == "5":
                name = get_user_input("User name to search: ")
                search_user(spark, name)
            
            elif choice == "6":
                query_users_spark_sql(spark)
            
            elif choice == "7":
                print("\n👋 Goodbye!")
                break
            
            else:
                print("⚠️ Enter a number 1–7.")
            
            input("\nPress Enter to continue...")
    
    finally:
        spark.stop()
        print("\n✅ SparkSession stopped.")

if __name__ == "__main__":
    main()
