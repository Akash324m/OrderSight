import os

if os.name == "nt":  # Windows
    os.environ["HADOOP_HOME"] = r"C:\Users\aakas\Downloads\hadoop"
    os.environ["hadoop.home.dir"] = r"C:\Users\aakas\Downloads\hadoop"
    os.environ["PATH"] += os.pathsep + r"C:\Users\aakas\Downloads\hadoop\winutils.exe"

from pyspark.sql import SparkSession, types, functions as F
import logging
import sys


# -----------------------------
# Logging Configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# -----------------------------
# Data Validation Function
# -----------------------------
def validate_orders(df):
    validation_results = {
        "null_order_id": df.filter(F.col("order_id").isNull()).count(),
        "null_order_date": df.filter(F.col("order_date").isNull()).count(),
        "invalid_price_each": df.filter(F.col("price_each") <= 0).count(),
        "invalid_quantity_ordered": df.filter(F.col("quantity_ordered") <= 0).count(),
        "null_product": df.filter(F.col("product").isNull()).count()
    }
    return validation_results


# -----------------------------
# Main Pipeline
# -----------------------------
def main():

    # 1. Initialize Spark Session
    spark = (
    SparkSession
    .builder
    .appName("cleaning_orders_dataset_with_pyspark")
    .config("spark.hadoop.io.native.lib", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .getOrCreate()
    )
    spark.conf.set("spark.hadoop.io.native.lib", "false")
    spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.conf.set("spark.hadoop.fs.permissions.enabled", "false")
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
    spark.conf.set("spark.sql.sources.commitProtocolClass",
                    "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")



    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized")

    # 2. Paths
    input_path = "Dataset\orders_data.parquet"
    clean_output_path = "Cleaned_Dataset\orders_data_clean.parquet"
    rejected_output_path = "Rejected_Data\orders_data_rejected.parquet"

    # 3. Load Data
    try:
        orders_data = spark.read.parquet(input_path)
        logger.info(f"Successfully loaded data from {input_path}")
    except Exception as e:
        logger.error(f"Failed to load input data: {e}")
        spark.stop()
        return

    # 4. Feature Engineering & Cleaning
    orders_data = (
        orders_data
        .withColumn(
            "time_of_day",
            F.when((F.hour("order_date") >= 0) & (F.hour("order_date") <= 5), "night")
             .when((F.hour("order_date") >= 6) & (F.hour("order_date") <= 11), "morning")
             .when((F.hour("order_date") >= 12) & (F.hour("order_date") <= 17), "afternoon")
             .when((F.hour("order_date") >= 18) & (F.hour("order_date") <= 23), "evening")
        )
        .filter(F.col("time_of_day") != "night")
        .withColumn("order_date", F.col("order_date").cast(types.DateType()))
        .withColumn("product", F.lower(F.col("product")))
        .withColumn("category", F.lower(F.col("category")))
        .filter(~F.col("product").contains("tv"))
    )

    # Extract purchase state
    orders_data = (
        orders_data
        .withColumn("address_split", F.split("purchase_address", " "))
        .withColumn(
            "purchase_state",
            F.col("address_split").getItem(F.size("address_split") - 2)
        )
        .drop("address_split")
    )

    logger.info("Data cleaning and feature engineering completed")

    # 5. Validation
    validation_results = validate_orders(orders_data)

    for rule, count in validation_results.items():
        logger.info(f"Validation check - {rule}: {count}")

    # 6. Separate Valid and Rejected Records
    rejected_records = orders_data.filter(
    (F.col("order_id").isNull()) |
    (F.col("order_date").isNull()) |
    (F.col("price_each") <= 0) |
    (F.col("quantity_ordered") <= 0) |
    (F.col("product").isNull())
)

    clean_records = orders_data.subtract(rejected_records)

    logger.info(f"Clean records count: {clean_records.count()}")
    logger.info(f"Rejected records count: {rejected_records.count()}")

    #debugging
    logger.info(f"Total records after cleaning: {orders_data.count()}")
    logger.info(f"Clean records count: {clean_records.count()}")
    logger.info(f"Rejected records count: {rejected_records.count()}")

    # 7. Write Output
    logger.info("Writing clean data")
    clean_records.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("orders_data_clean_csv")


    logger.info("Writing rejected data")
    rejected_records.write.parquet(rejected_output_path, mode="overwrite")

    logger.info("Pipeline execution completed successfully")

    spark.stop()


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    main()
