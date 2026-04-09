# 🛒 Retail Real-time Analytics Pipeline (H&M Dataset)

Hệ thống quản lý và phân tích lịch sử giao dịch bán lẻ dựa trên nền tảng Big Data. Dự án sử dụng dữ liệu thực tế để mô phỏng luồng giao dịch từ các máy POS về kho lưu trữ tập trung, cho phép truy vấn thống kê doanh thu theo thời gian thực (Real-time).

**Nhóm thực hiện:** Nhóm 14  
**Sinh viên:** Trần Phạm Khánh Duy (2033230043) & Lê Hữu Luân (2001230482)

---

## 🏗️ Kiến trúc Hệ thống
Luồng dữ liệu thiết kế theo mô hình xử lý luồng (Stream Processing):  
**POS Simulator (Python) → Apache Kafka → Apache Spark Streaming → Apache HBase → Apache Phoenix → SQL Analytics**.

---

## 💻 Yêu cầu hệ thống (Prerequisites)
Để chạy dự án trên máy cá nhân (Local/Windows), bạn cần cài đặt sẵn:
1. **[Docker Desktop](https://www.docker.com/products/docker-desktop/)**: Đang chạy (Icon cá voi màu xanh).
2. **Python 3.8 - 3.11**: (Lưu ý: Spark hiện chưa tương thích tốt với Python 3.12+).
3. **Java 8 hoặc 11**: Thiết lập biến môi trường `JAVA_HOME`.
4. **Hadoop Winutils (Chỉ dành cho Windows)**: 
   * Tải thư mục `hadoop` (chứa `winutils.exe` và `hadoop.dll`).
   * Đặt vào ổ C (ví dụ: `C:\hadoop`).
   * Cài đặt biến môi trường: `HADOOP_HOME = C:\hadoop` và thêm `C:\hadoop\bin` vào `PATH`.

---

## 🛠️ Hướng dẫn Cài đặt & Thiết lập

### Bước 1: Chuẩn bị Dữ liệu (Dataset)
* Tải bộ dữ liệu gốc từ [Kaggle H&M Dataset](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data).
* Giải nén, lấy file `transactions_train.csv` và đặt vào đúng đường dẫn: `Data/transactions_train.csv`.

### Bước 2: Khởi động Hạ tầng (Docker)
1. Mở Terminal/CMD, di chuyển vào thư mục `docker` của dự án:
   ```bash
   cd docker
   ```
2. Dựng cụm Zookeeper, Kafka và HBase:
   ```bash
   docker-compose up -d
   ```
3. Chờ 30 giây cho các services lên hoàn toàn.

### Bước 3: Mở cổng giao tiếp cho Python (Thrift Server)
Bắt buộc kích hoạt Thrift để thư viện HappyBase của Python có thể ghi dữ liệu xuống HBase. Chạy lệnh:
```bash
docker exec docker-hbase-1 /opt/hbase/bin/hbase-daemon.sh start thrift
```

### Bước 4: Khởi tạo Lớp SQL (Apache Phoenix)
Truy cập vào giao diện Phoenix:
```bash
docker exec -it docker-hbase-1 /opt/phoenix/bin/sqlline.py localhost
```
Dán lệnh SQL sau để tạo bảng. Bảng được cấu hình chia 12 luồng (`SPLIT ON`) để chống nghẽn nút (Hotspotting) và tắt mã hóa cột (`COLUMN_ENCODED_BYTES = 0`) để tương thích với Python:
```sql
DROP TABLE IF EXISTS "transactions";

CREATE TABLE "transactions" (
    "rowkey" VARCHAR PRIMARY KEY,
    "cf"."store_id" VARCHAR,
    "cf"."timestamp" VARCHAR,
    "cf"."total_amount" VARCHAR
) COLUMN_ENCODED_BYTES = 0 
SPLIT ON ('1_','2_','3_','4_','5_','6_','7_','8_','9_','10_','11_');
```

### Bước 5: Cài đặt Thư viện Python
Mở Terminal mới cài đặt các gói cần thiết:
```bash
pip install pyspark==3.5.0 kafka-python happybase
```

---

## 🚀 Hướng dẫn Vận hành Hệ thống

Để hệ thống chạy mượt mà không gặp lỗi lệch pha dữ liệu, hãy chạy ĐÚNG THỨ TỰ sau:

**1. Xóa Checkpoint cũ (Nếu chạy lại)** Nếu đây không phải lần chạy đầu tiên, bạn **BẮT BUỘC** phải xóa thư mục lưu Checkpoint của Spark (Ví dụ: `D:/spark_checkpoint_final`) để Spark đọc lại dữ liệu Kafka từ đầu.

**2. Bật Máy lọc dữ liệu (Spark Streaming)** Mở Terminal 1 tại thư mục gốc dự án:
```bash
python ingestion/spark_streaming.py
```
*(Đợi đến khi màn hình báo "HỆ THỐNG SPARK ĐÃ KHỞI ĐỘNG...")*

**3. Bật Máy bơm dữ liệu (Kafka Producer)** Mở Terminal 2 tại thư mục gốc dự án:
```bash
python ingestion/kafka_producer.py
```
*(Hệ thống bắt đầu đọc file CSV và đẩy 1 triệu dòng vào Kafka. Màn hình Spark sẽ hiển thị tiến độ ghi vào HBase theo từng Micro-batch).*

---

## 📈 Truy vấn Phân tích Real-time

Quay lại cửa sổ Terminal của Phoenix (ở Bước 4). Trong lúc Spark đang chạy, gõ lệnh sau để xem bảng xếp hạng doanh thu 100 cửa hàng thay đổi theo từng giây:
```sql
SELECT "store_id", 
       COUNT(*) AS so_giao_dich, 
       SUM(TO_NUMBER("total_amount")) AS doanh_thu
FROM "transactions"
GROUP BY "store_id"
ORDER BY doanh_thu DESC;
```
*(Mẹo: Bấm mũi tên ⬆️ và `Enter` liên tục để cập nhật Dashboard số liệu mới nhất).*
```
