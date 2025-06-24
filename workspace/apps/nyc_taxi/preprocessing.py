import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, hour, dayofweek, month

BUCKET_NAME = "nyc-taxi-data" 
RAW_DATA_PATH = f"s3a://{BUCKET_NAME}/raw/yellow_tripdata_*.parquet"
PROCESSED_DATA_PATH = f"s3a://{BUCKET_NAME}/processed/cleaned_yellow_taxi.parquet"

#RAW_DATA_PATH = "../../data/raw/yellow_tripdata_*.parquet"
#PROCESSED_DATA_PATH = "../../data/processed/cleaned_yellow_taxi.parquet"

def create_spark_session(app_name="NYC Taxi Data Preprocessing"):
    """
    Create a Spark session with the necessary configurations.
    """
    
    # spark = (
    #     SparkSession.builder
    #     .appName(app_name)
    #     .master("local[*]")  # Use all available cores on the local machine
    #     .config("spark.executor.memory", "6g") \
    #     .config("spark.driver.memory", "10g") \
    #     .config("spark.sql.shuffle.partitions", "8") \
    #     .getOrCreate()
    # )

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
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    return spark



def load_data(spark):
    """
    Load the raw NYC taxi data from Parquet files.
    """
    df = spark.read.parquet(RAW_DATA_PATH)

    columns_subset = [
        'tpep_pickup_datetime', 
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'PULocationID',
        'DOLocationID',
        'fare_amount',
        'total_amount'
    ]

    return df.select(*columns_subset)


def parse_dates(df):
    """
    Parse date columns to timestamp format.
    """
    df = df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
           .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    return df.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")


def filter_invalid_data(df):
    """Filtra righe non valide (valori nulli o negativi)"""
    return df.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0) &
        (col("passenger_count").between(0, 6)) &
        ((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) > 0)
    )


def compute_new_features(df):
    """Calcola nuove feature utili per l'analisi"""
    df = df.withColumn("trip_duration_min", 
                       (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)

    df = df.filter(col("trip_duration_min") < 180)  # Massimo 3 ore

    df = df.withColumn("avg_speed_mph", 
                       col("trip_distance") / (col("trip_duration_min") / 60))

    df = df.withColumn("pickup_hour", hour("pickup_datetime")) \
           .withColumn("pickup_day_of_week", dayofweek("pickup_datetime")) \
           .withColumn("pickup_month", month("pickup_datetime"))
    
    return df

def rename_and_reorder_columns(df):
    """Rinomina e riordina le colonne per chiarezza"""
    df = df.withColumnRenamed("PULocationID", "pickup_zone") \
           .withColumnRenamed("DOLocationID", "dropoff_zone")

    return df.select(
        "pickup_datetime", "dropoff_datetime",
        "pickup_hour", "pickup_day_of_week", "pickup_month",
        "passenger_count", "trip_distance", "trip_duration_min", "avg_speed_mph",
        "fare_amount", "total_amount",
        "pickup_zone", "dropoff_zone"
    )

def save_processed_data(df):
    """Salva i dati preprocessati in formato parquet"""
    output_path = PROCESSED_DATA_PATH
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Dati preprocessati salvati in: {output_path}")

def main():
    """
    Main function to run the preprocessing steps.
    """
    try:

        print("🚀 Avvio del processo di preprocessing...")
        spark = create_spark_session()
        
        print("📥 Caricamento e selezione colonne...")
        df = load_data(spark)

        print("📅 Parsing delle date...")
        df = parse_dates(df)

        print("🧹 Filtro dati non validi...")
        df = filter_invalid_data(df)

        print("🧮 Calcolo nuove feature...")
        df = compute_new_features(df)

        print("🔄 Rinomina e riordino colonne...")
        df = rename_and_reorder_columns(df)

        print("💾 Salvataggio dati preprocessati...")
        save_processed_data(df)

        print("🎉 Preprocessing completato!")

        # Keep the Spark Driver alive so you can browse the UI at http://localhost:4040
        print("\nApplication finished. Driver is still alive.")
        input("Press Enter to stop the Spark session and exit...\n")
    
    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("✅ Spark session stopped successfully.")


if __name__ == "__main__":
    main()    