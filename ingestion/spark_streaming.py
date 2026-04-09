import os
import sys
import happybase

# Cấu hình môi trường Windows
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def write_to_hbase(batch_df, batch_id):
    """Hàm ghi dữ liệu thẳng vào HBase thông qua HappyBase"""
    rows = batch_df.collect()
    if not rows:
        return
        
    try:
        # Đảm bảo bạn đã chạy: hbase thrift start -p 9090
        connection = happybase.Connection('127.0.0.1', port=9090, timeout=60000)
        table = connection.table('transactions')
        
        with table.batch(batch_size=200) as b:
            for row in rows:
                b.put(
                    row['rowkey'].encode(),
                    {
                        b'cf:_0': b'x', # Đóng mộc cho Phoenix để xài lệnh COUNT/SUM
                        b'cf:store_id': str(row['store_id']).encode(),
                        b'cf:timestamp': str(row['timestamp']).encode(),
                        b'cf:total_amount': str(row['total_amount']).encode()
                    },
                    wal=False # Tắt WAL để Max tốc độ ghi
                )
        connection.close()
        print(f">>> Batch {batch_id} hoàn tất: Lấy {len(rows)} dòng từ Kafka đẩy vào HBase.")
        
    except Exception as e:
        print(f"Lỗi HBase: {e}")

def main():
    # Khởi tạo Spark tối ưu đa luồng
    spark = SparkSession.builder \
        .appName("RetailTransactionProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")

    # 1. Định nghĩa đúng Schema theo file Producer
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("store_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("total_amount", DoubleType())
    ])

    # 2. Đọc luồng từ Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "pos_transactions") \
        .option("startingOffsets", "earliest") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 3. Ép kiểu JSON và chế tạo RowKey 12 luồng
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*") \
        .filter(col("total_amount") > 0) \
        .withColumn(
            "rowkey",
            concat(
                (abs(hash(col("transaction_id"))) % 12).cast("string"), # Băm 12 luồng
                lit("_"),
                col("store_id"),
                lit("_"),
                unix_timestamp(col("timestamp")).cast("string"),
                lit("_"),
                substring(col("transaction_id"), -8, 8) # Lấy 8 ký tự cuối của txn_id cho an toàn
            )
        )

    # 4. Kích hoạt luồng ghi
    print(">>> HỆ THỐNG SPARK ĐÃ KHỞI ĐỘNG (Đợi dữ liệu từ Producer)...")
    query = parsed_df.writeStream \
        .foreachBatch(write_to_hbase) \
        .trigger(processingTime='3 seconds') \
        .option("checkpointLocation", "D:/spark_checkpoint_final_version") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()