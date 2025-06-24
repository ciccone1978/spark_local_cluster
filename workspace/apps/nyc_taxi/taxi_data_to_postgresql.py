import os
from pyspark.sql import SparkSession

def create_cluster_spark_session():
    """Creates a Spark session configured for a large data processing job."""
    return (
        SparkSession.builder
        .appName("Parquet-to-Postgres-Loader")
        .master("spark://spark-master:7077")
        .config("spark.driver.host", "jupyterlab")
        .config("spark.ui.port", "4040")
        .config("spark.driver.memory", "4g")  # Increase driver memory as it coordinates a large write operation
        .getOrCreate()
    )

def main():
    spark = create_cluster_spark_session()
    
    # --- Source and Destination Configuration ---
    
    # 1. Source: The location of your large Parquet file(s) in MinIO
    BUCKET_NAME = "nyc-taxi-data" 
    PROCESSED_DATA_PATH = f"s3a://{BUCKET_NAME}/processed/cleaned_yellow_taxi.parquet"
    
    # 2. Destination: PostgreSQL connection details
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_db = os.getenv("POSTGRES_DB")
    postgres_url = f"jdbc:postgresql://postgres-gis:5432/{pg_db}"
    
    # The new table will be named 'trips' inside the 'nyctaxi' schema
    table_name = "nyctaxi.trips" 
    
    connection_properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }

    try:
        # --- Job Execution ---
        
        print(f"Reading Parquet data from: {PROCESSED_DATA_PATH}")
        df = spark.read.parquet(PROCESSED_DATA_PATH)
        
        # Optional but Recommended: Select only necessary columns
        # This reduces the amount of data written to Postgres
        columns_to_keep = [
            "pickup_datetime", "dropoff_datetime",
            "passenger_count", "trip_distance", "pickup_zone", "dropoff_zone",
            "fare_amount", "total_amount"
        ]
        df_to_write = df.select(*columns_to_keep)

        print("Data schema to be written to PostgreSQL:")
        df_to_write.printSchema()
        print(f"Total rows to write: {df_to_write.count()}")
        
        print(f"Writing data to PostgreSQL table: {table_name}")
        
        # --- Writing the DataFrame to PostgreSQL ---
        (df_to_write.write
            .mode("overwrite")
            .option("batchsize", 100000)
            .option("numPartitions", 20)
            .jdbc(
                url=postgres_url,
                table=table_name,
                properties=connection_properties
            )
        )

        print("✅ Successfully wrote one year of NYC taxi data to PostgreSQL!")

    except Exception as e:
        print(f"\n❌ AN ERROR OCCURRED: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Keep the driver alive so you can inspect the Spark UI if needed
        input("\nPress Enter to stop the Spark session and exit...")
        spark.stop()

if __name__ == "__main__":
    main()