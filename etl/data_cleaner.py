# etl/data_cleaner.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class DataCleaner:
    def __init__(self, spark_session=None):
        self.spark = spark_session or SparkSession.builder \
            .appName("DataCleaner") \
            .getOrCreate()

    def clean_transactions(self, df):
        """Full data cleaning pipeline"""
        df = self.remove_invalid_records(df)
        df = self.deduplicate_transactions(df)
        df = self.standardize_formats(df)
        df = self.validate_business_rules(df)
        df = self.enrich_data(df)
        return df

    def remove_invalid_records(self, df):
        """Loại bỏ các bản ghi thiếu dữ liệu hoặc không hợp lệ"""
        return df.filter(
            col("transaction_id").isNotNull() &
            col("store_id").isNotNull() &
            col("timestamp").isNotNull() &
            (col("total_amount") > 0) &
            (col("total_amount") < 1000000)  # Sanity check
        )

    def deduplicate_transactions(self, df):
        """Xóa giao dịch trùng lặp, giữ lại giao dịch mới nhất"""
        window_spec = Window.partitionBy("transaction_id") \
            .orderBy(col("timestamp").desc())
        return df.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

    def standardize_formats(self, df):
        """Chuẩn hóa định dạng cửa hàng và phương thức thanh toán"""
        return df.withColumn(
            "store_id",
            regexp_replace(upper(col("store_id")), "^A-Z0-9_", "")
        ).withColumn(
            "payment_method",
            when(col("payment_method").isin("CASH", "cash"), "CASH")
            .when(col("payment_method").isin("CARD", "card", "credit"), "CARD")
            .when(col("payment_method").isin("MOBILE", "mobile", "wallet"), "MOBILE")
            .otherwise("OTHER")
        ).withColumn(
            "total_amount", round(col("total_amount"), 2)
        )

    def validate_business_rules(self, df):
        """Đánh dấu các giao dịch đáng ngờ (ví dụ: số tiền lớn, thanh toán không rõ)"""
        df = df.withColumn(
            "is_suspicious",
            (col("total_amount") > 5000) | 
            (col("payment_method") == "OTHER") | 
            (col("store_id").isNull())
        )
        return df.withColumn(
            "validation_status",
            when(col("is_suspicious"), "REVIEW").otherwise("VALID")
        )

    def enrich_data(self, df):
        """Bổ sung các trường dữ liệu như ngày, giờ, cuối tuần, phân loại số tiền"""
        return df.withColumn(
            "transaction_date", to_date(col("timestamp"))
        ).withColumn(
            "transaction_hour", hour(col("timestamp"))
        ).withColumn(
            "transaction_day", dayofweek(col("timestamp"))
        ).withColumn(
            "is_weekend",
            when(col("transaction_day").isin(1, 7), True).otherwise(False)
        ).withColumn(
            "amount_category",
            when(col("total_amount") < 50, "SMALL")
            .when(col("total_amount") < 200, "MEDIUM")
            .when(col("total_amount") < 1000, "LARGE")
            .otherwise("XLARGE")
        )