# etl/batch_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from data_cleaner import DataCleaner
from pyspark.sql.types import StringType
class HistoricalDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("HistoricalDataProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        self.cleaner = DataCleaner(self.spark)

    def process_csv_files(self, input_path, output_table):
        """Đọc CSV, làm sạch, phân vùng và Bulk Load vào HBase"""
        # Đọc dữ liệu
        df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        # Làm sạch
        cleaned_df = self.cleaner.clean_transactions(df)
        
        # Thêm rowkey và repartition để tối ưu ghi
        cleaned_df = self.add_rowkey(cleaned_df)
        cleaned_df = cleaned_df.repartition(16, col("store_id"))
        
        # Ghi vào HBase
        self.bulk_load_to_hbase(cleaned_df, output_table)
        return cleaned_df.count()

    def add_rowkey(self, df):
        """Thêm Row Key đã thiết kế"""
        def create_key(store_id, timestamp, txn_id):
            return f"{store_id}_{timestamp}_{txn_id[-8:]}"
        create_key_udf = udf(create_key, StringType())
        return df.withColumn("rowkey", create_key_udf(col("store_id"), col("timestamp"), col("transaction_id")))

    def bulk_load_to_hbase(self, df, table_name):
        """Bulk load vào HBase sử dụng định dạng org.apache.hadoop.hbase.spark"""
        df.write \
            .format("org.apache.hadoop.hbase.spark") \
            .option("hbase.table", table_name) \
            .option("hbase.columns.mapping", 
                    "rowkey string :key, " +
                    "cf_detail.store_id string, " +
                    "cf_detail.amount string, " +
                    "cf_detail.payment string, " +
                    "cf_meta.customer_id string, " +
                    "cf_meta.timestamp string") \
            .option("hbase.spark.use.hbasecontext", "false") \
            .option("hbase.fs.defaultFS", "hdfs://localhost:9000") \
            .mode("append") \
            .save()
        print(f"Bulk loaded data to HBase table {table_name}")

    def aggregate_daily_sales(self, df):
        """Tiền tổng hợp (Pre-aggregate) doanh thu theo ngày giúp truy vấn nhanh hơn"""
        return df.groupBy("store_id", "transaction_date").agg(
            count("transaction_id").alias("transaction_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_value"),
            collect_set("payment_method").alias("payment_methods")
        ).orderBy("store_id", "transaction_date")