import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_spark_session():
    # Helper to create our standard cluster session
    return (
        SparkSession.builder
        .appName("PostgresConnectivityTest")
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

def main():
    spark = get_spark_session()
    
    # --- PostgreSQL Connection Properties ---
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_db = os.getenv("POSTGRES_DB")

    postgres_url = f"jdbc:postgresql://postgres-gis:5432/{pg_db}"
    connection_properties = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }

    try:
        # 1. Create a sample DataFrame to write
        print("- Creating sample data...")
        data = [("Electronics", 1500.50), ("Clothing", 450.25), ("Home", 750.00)]
        columns = ["category", "total_sales"]
        df_to_write = spark.createDataFrame(data, columns)
        
        # 2. Write the DataFrame to a PostgreSQL table
        table_name = "nyctaxi.sales_summary"
        print(f"- Writing data to PostgreSQL table '{table_name}'...")
        df_to_write.write.jdbc(
            url=postgres_url,
            table=table_name,
            mode="overwrite", # "overwrite" will drop and recreate the table
            properties=connection_properties
        )
        print("✅ Write operation successful.")
        
        # 3. Read the data back from the PostgreSQL table to verify
        print(f"\n- Reading data back from table '{table_name}'...")
        df_read_back = spark.read.jdbc(
            url=postgres_url,
            table=table_name,
            properties=connection_properties
        )
        print("✅ Read operation successful.")
        
        # 4. Show the result
        print("\n- Data read from PostgreSQL:")
        df_read_back.show()
        
    except Exception as e:
        print(f"\n❌ AN ERROR OCCURRED: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\n- Stopping Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()