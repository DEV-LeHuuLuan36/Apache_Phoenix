# ingestion/kafka_producer.py

import csv
import json
import time
from kafka import KafkaProducer


def send_hm_data_to_kafka(csv_file_path, target_count=1_000_000):
    """
    Stream dữ liệu từ file CSV (H&M dataset) và gửi vào Kafka topic.

    Parameters:    - csv_file_path: đường dẫn file CSV
    - target_count: số bản ghi tối đa cần gửi
    """

    # Kafka Producer cấu hình tối ưu cho throughput cao
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=32768,   # tăng kích thước batch để giảm số lần gửi
        linger_ms=20        # delay nhỏ để gom batch hiệu quả hơn
    )

    print(f"START: Reading data from {csv_file_path}")
    start_time = time.time()
    count = 0

    try:
        # Đọc file theo streaming (tránh load toàn bộ vào RAM)
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:

                # ==============================
                # DATA MAPPING (H&M -> POS Schema)
                # ==============================

                # Transaction ID: đảm bảo unique
                txn_id = f"{row['customer_id'][:10]}_{count}"

                # Store ID: phân phối đều vào 100 store
                try:
                    store_num = (int(row['article_id']) % 100) + 1
                except ValueError:
                    store_num = (count % 100) + 1

                store_id = f"STORE_{store_num:03d}"

                # Timestamp: bổ sung thời gian vào dữ liệu chỉ có ngày
                hour = (count % 14) + 8
                minute = count % 60
                second = count % 60

                timestamp = f"{row['t_dat']} {hour:02d}:{minute:02d}:{second:02d}"

                #scale giá trị lên để giống dữ liệu POS thực tế
                total_amount = round(float(row['price']) * 10000, 2)

                # ==============================
                # BUILD EVENT PAYLOAD
                # ==============================
                transaction = {
                    "transaction_id": txn_id,
                    "store_id": store_id,
                    "timestamp": timestamp,
                    "total_amount": total_amount
                }

                # Gửi vào Kafka topic
                producer.send("pos_transactions", value=transaction)
                count += 1

                # Log tiến độ (giảm tần suất để tránh I/O overhead)
                if count % 50000 == 0:
                    print(f"PROGRESS: {count:,} records sent")

                if count >= target_count:
                    break

    except FileNotFoundError:
        print(f"ERROR: File not found -> {csv_file_path}")
        print("ACTION: Update correct path in REAL_DATA_FILE")
        return

    # Đẩy toàn bộ buffer còn lại
    print("FLUSH: Sending remaining buffered messages...")
    producer.flush()

    end_time = time.time()
    duration = round(end_time - start_time, 2)

    print("COMPLETE")
    print(f"Records sent     : {count:,}")
    print(f"Total time (sec) : {duration}")
    print(f"Throughput (TPS) : {round(count / duration, 2)}")

    producer.close()


if __name__ == "__main__":

    # Cập nhật đúng đường dẫn file CSV
    REAL_DATA_FILE = "D:/Python/Apache_Phoenix/Data/transactions_train.csv"

    # Số lượng record cần gửi
    send_hm_data_to_kafka(
        csv_file_path=REAL_DATA_FILE,
        target_count=1_000_000
    )