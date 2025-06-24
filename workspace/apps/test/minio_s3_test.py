import sys
from pyspark.sql import SparkSession

# This is a good practice if you have helper modules in other directories
# For this script, it's not strictly necessary but shows a common pattern.
# sys.path.append('..') 

def create_cluster_spark_session(app_name="S3ConnectivityTest"):
    """
    Creates a Spark session configured to connect to the local Docker cluster.
    S3/MinIO configurations are automatically loaded from spark-defaults.conf.
    """
    print("Attempting to create Spark session...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.driver.host", "jupyterlab")
        .config("spark.ui.port", "4040")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") 
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created successfully.")
    return spark


def main():
    """
    Main function to test writing to and reading from MinIO S3.
    """
    spark = create_cluster_spark_session()
    
    # 1. DEFINE YOUR S3 BUCKET AND PATH
    # Ensure you have created this bucket in the MinIO UI (http://localhost:9001)
    bucket_name = "nyc-taxi-data" 
    output_path = f"s3a://{bucket_name}/test/s3_connectivity_test.parquet"
    
    print(f"\n- Target S3 Path: {output_path}")
    
    try:
        # 2. CREATE A SAMPLE DATAFRAME
        print("- Creating sample data...")
        data = [("Alice", 34, "Engineer"),
                ("Bob", 45, "Manager"),
                ("Charlie", 29, "Analyst")]
        columns = ["name", "age", "role"]
        df_to_write = spark.createDataFrame(data, columns)
        print("  Sample data created:")
        df_to_write.show()
        
        # 3. WRITE THE DATAFRAME TO MINIO S3
        print(f"- Writing data to MinIO at {output_path}...")
        df_to_write.write.mode("overwrite").parquet(output_path)
        print("✅ Write operation completed successfully.")
        
        # 4. READ THE DATA BACK FROM MINIO S3 TO VERIFY
        print(f"\n- Reading data back from {output_path} to verify...")
        df_read_back = spark.read.parquet(output_path)
        print("✅ Read operation completed successfully.")
        
        # 5. VALIDATE THE DATA
        print("\n- Validating the data read back from S3:")
        print("  Schema of the data from S3:")
        df_read_back.printSchema()
        
        print("  Content of the data from S3:")
        df_read_back.show()
        
        assert df_to_write.count() == df_read_back.count(), "Row count mismatch!"
        print("\n🎉 VALIDATION SUCCESS: Row counts match.")
        
    except Exception as e:
        print(f"\n❌ AN ERROR OCCURRED: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\n- Stopping Spark session...")
        spark.stop()
        print("✅ Spark session stopped.")


if __name__ == "__main__":
    main()