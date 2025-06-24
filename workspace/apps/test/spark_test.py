"""
PySpark script that demonstrates core functionalities.
Can be configured to run in either:
1. True local mode (no cluster required) for simple, fast testing.
2. Cluster mode to connect to the Docker-based Spark cluster.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, when, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# --- A dedicated function for true local mode ---
def create_local_spark_session(app_name="LocalSparkTest"):
    """
    Creates a Spark session designed to run in true local mode.
    - No master URL is specified, so it defaults to local[*].
    - No cluster-specific configs are needed.
    - Perfect for development, unit tests, and running without a cluster.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Use all available cores on the local machine
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_cluster_spark_session(app_name="ClusterSparkTest"):
    """
    Creates a Spark session configured to connect to the local Docker cluster.
    - Specifies the master URL and driver host for the Docker network.
    - Aligns executor resources with the docker-compose configuration.
    """
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
    return spark


def create_sample_data(spark):
    """Create sample sales data for testing"""
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("customer_age", IntegerType(), True),
        StructField("region", StringType(), True)
    ])
    
    data = [
        (1, "Laptop", "Electronics", 999.99, 1, 28, "North"),
        (2, "Coffee Mug", "Home", 12.99, 3, 34, "South"),
        (3, "Smartphone", "Electronics", 699.99, 1, 25, "East"),
        (4, "Book", "Education", 24.99, 2, 42, "West"),
        (5, "Headphones", "Electronics", 199.99, 1, 30, "North"),
        (6, "T-Shirt", "Clothing", 19.99, 4, 26, "South"),
        (7, "Tablet", "Electronics", 399.99, 1, 35, "East"),
        (8, "Notebook", "Education", 8.99, 5, 28, "West"),
        (9, "Watch", "Accessories", 299.99, 1, 45, "North"),
        (10, "Shoes", "Clothing", 89.99, 2, 33, "South")
    ]
    
    return spark.createDataFrame(data, schema)


def perform_data_analysis(df):
    """Perform comprehensive data analysis"""
        
    # Basic info
    print("Dataset Overview:")
    df.show(5, truncate=False)
    print(f"Total transactions: {df.count()}")
    print(f"Total columns: {len(df.columns)}")
    
    # Schema info
    print("\nDataset Schema:")
    df.printSchema()
    
    # Add calculated columns
    df_with_total = df.withColumn("total_amount", col("price") * col("quantity"))
    
    print("\nData with Total Amount:")
    df_with_total.select("product", "price", "quantity", "total_amount").show()
    
    # Category analysis
    print("\nCategory Analysis:")
    category_stats = df_with_total.groupBy("category") \
        .agg(
            count("*").alias("transaction_count"),
            avg("price").alias("avg_price"),
            avg("total_amount").alias("avg_total")
        ) \
        .orderBy(desc("avg_total"))
    
    category_stats.show()
    
    # Regional analysis
    print("\nRegional Sales Analysis:")
    regional_stats = df_with_total.groupBy("region") \
        .agg(
            count("*").alias("transactions"),
            sum("total_amount").alias("total_sales")
        ) \
        .orderBy(desc("total_sales"))
    
    regional_stats.show()
    
    # Customer age segmentation
    print("\nCustomer Age Segmentation:")
    age_segments = df_with_total.withColumn(
        "age_group",
        when(col("customer_age") < 30, "Young")
        .when(col("customer_age") < 40, "Middle")
        .otherwise("Senior")
    )
    
    age_analysis = age_segments.groupBy("age_group") \
        .agg(
            count("*").alias("customers"),
            avg("total_amount").alias("avg_spending")
        ) \
        .orderBy("age_group")
    
    age_analysis.show()
    
    # Top products by revenue
    print("\nTop Products by Revenue:")
    top_products = df_with_total.select("product", "total_amount") \
        .orderBy(desc("total_amount")) \
        .limit(5)
    
    top_products.show()
    
    return df_with_total


def demonstrate_spark_sql(df, spark):
    """Demonstrate Spark SQL capabilities"""
    print("\n=== Spark SQL Demo ===")
    
    # Register DataFrame as temporary view
    df.createOrReplaceTempView("sales")
    
    # SQL query
    sql_result = spark.sql("""
        SELECT 
            category,
            COUNT(*) as transaction_count,
            ROUND(AVG(price * quantity), 2) as avg_revenue,
            ROUND(SUM(price * quantity), 2) as total_revenue
        FROM sales 
        GROUP BY category
        ORDER BY total_revenue DESC
    """)
    
    print("SQL Query Results:")
    sql_result.show()


def main():
    """Main function"""
    
    # To run on the Docker cluster, use this:
    spark = create_cluster_spark_session()
    
    # To run in true local mode (no cluster needed), use this:
    # spark = create_local_spark_session()
    
    print(f"🚀 Starting PySpark Test on master: {spark.sparkContext.master}")
    
    try:
        print(f"✅ Spark Version: {spark.version}")
        print(f"✅ Spark Master: {spark.sparkContext.master}")
        print(f"✅ App Name: {spark.sparkContext.appName}")
        
        # Create and analyze data
        df = create_sample_data(spark)
        df_analyzed = perform_data_analysis(df)
        
        # Demonstrate SQL
        demonstrate_spark_sql(df_analyzed, spark)
               
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\n✅ Spark session stopped successfully.")


if __name__ == "__main__":
    main()