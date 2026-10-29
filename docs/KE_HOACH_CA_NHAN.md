# Kế hoạch cá nhân – Đồ án Nhập môn Dữ liệu lớn (làm 1 mình)
Quy trình: **Storage → Processing** | Dữ liệu: NYC TLC Yellow Taxi 12 tháng (+1 tháng HVFHV, +tới 24 tháng nếu máy chịu được) | Công nghệ: PySpark, Spark SQL, Parquet
Thời hạn: 18/10 → ~01/11/2026 (14 ngày, trong đó 2 ngày cuối là dự phòng). Máy 8 GB RAM.

## 1. Nguyên tắc khi làm một mình
1. **Làm dọc, không làm ngang:** mỗi giai đoạn xong phải có (code chạy được + số liệu/ảnh + 1 đoạn báo cáo). Không để "viết báo cáo" dồn vào cuối.
2. **Phân tầng ưu tiên** để biết cắt gì khi trễ (mục 2).
3. **Tự giải thích được:** đề đánh giá việc hiểu hệ thống. Mỗi tối tự trả lời 3 câu: job này nhận gì, làm gì, ghi ra gì.
4. **Ghi log tự động:** mọi số đo ghi vào `results/*.csv` bằng script, không chép tay.

## 2. Phạm vi theo mức ưu tiên

| Mức | Hạng mục | Điểm liên quan |
|---|---|---|
| **BẮT BUỘC** | Tải dữ liệu, ghi Parquet có phân vùng, data contract | Hoàn thành hệ thống (2.5) |
| | Job làm sạch + bảng chất lượng trước/sau | Triển khai (2.0) |
| | 5–6 truy vấn KPI Spark SQL có join bảng khu vực | Hoàn thành hệ thống |
| | Benchmark theo cỡ dữ liệu (1/3/6/12 tháng) và theo số partition | Thực nghiệm (2.0), Big Data (1.5) |
| | Báo cáo theo cấu trúc mục 6.1, README, phụ lục AI, bảng đóng góp | Báo cáo (1.0), Phân tích (1.0) |
| **NÊN CÓ** | CSV so với Parquet; Pandas so với Spark trên cùng dữ liệu; ảnh Spark UI | Thực nghiệm, Big Data |
| | 1 tháng HVFHV để thử scale | Big Data |
| **NẾU CÒN THỜI GIAN** | Docker Compose tái lập; mở rộng 24 tháng; Delta Lake | Tái lập |

Cắt theo thứ tự: Delta → 24 tháng → Docker → HVFHV. Không cắt phần bắt buộc.

## 3. Lịch từng ngày

| Ngày | Việc kỹ thuật | Viết báo cáo cùng ngày |
|---|---|---|
| 18/10 | Cài JDK 17, Python venv, `pip install pyspark pandas pyarrow`, chạy job kiểm tra Spark | Đọc kỹ đề, tạo khung báo cáo |
| 19/10 | `scripts/download_data.py`: tải Yellow 12 tháng + taxi zone lookup | Mục 3: nguồn dữ liệu, quy mô |
| 20/10 | `jobs/01_ingest_to_lake.py`: ghi Parquet phân vùng theo năm/tháng, kiểm tra số dòng; viết `data_contract.md` | Mục 3: schema, ví dụ bản ghi |
| 21/10 | `jobs/02_clean.py`: luật lỗi (khoảng cách ≤ 0, giá âm, giờ trả < giờ đón, ngoài tháng, vô lý về hành khách) | Mục 2, 4.2 |
| 22/10 | Chạy làm sạch trên 12 tháng, lập `dq_report.csv` trước/sau | Mục 6.1 (chất lượng dữ liệu) |
| 23/10 | `jobs/03_aggregate.py` + `sql/kpi_*.sql` (join zone lookup) | Mục 4.3 |
| 24/10 | Kiểm tra tính đúng KPI (đối chiếu mẫu nhỏ bằng Pandas), vẽ biểu đồ | Mục 7 (kết quả) |
| 25/10 | `benchmarks/run_benchmark.py`: cỡ dữ liệu 1/3/6/12 tháng | Mục 6 (kịch bản thí nghiệm) |
| 26/10 | Benchmark số partition, CSV vs Parquet, Pandas vs Spark; chụp Spark UI | Mục 6 (kết quả) |
| 27/10 | Phân tích số đo, tải thêm 1 tháng HVFHV nếu máy ổn | Mục 8 (thảo luận) |
| 28/10 | Vẽ sơ đồ data-flow, viết README + `requirements.txt` (Docker nếu kịp) | Mục 4.1, 5 |
| 29/10 | Ghép báo cáo hoàn chỉnh | Bìa, Tóm tắt, Mục 1, 9, Phụ lục |
| 30/10 | **Chạy lại toàn bộ từ thư mục sạch** theo README, sửa lỗi | Chỉnh số liệu cho khớp |
| 31/10 | Rà soát, xuất PDF, tự vấn đáp | Hoàn thiện |
| 01/11 | Dự phòng, đóng gói, nộp | |

## 4. Cấu hình cho máy 8 GB
- PySpark chạy local: `local[2]`, `spark.driver.memory=3g`, `spark.sql.shuffle.partitions=16`.
- Đóng trình duyệt nhiều tab và IDE nặng khi benchmark để số đo ổn định.
- Benchmark tăng dần 1 → 3 → 6 → 12 tháng; nếu máy bắt đầu swap thì dừng và ghi giới hạn vào báo cáo.
- Chạy mỗi cấu hình 3 lần, lấy trung bình và độ lệch để có số đo đáng tin.

## 5. Cấu trúc thư mục
```
bigdata_project/
├── README.md
├── NOTES.md                 # nhật ký làm việc
├── requirements.txt
├── scripts/download_data.py
├── jobs/{01_ingest_to_lake,02_clean,03_aggregate}.py
├── sql/kpi_*.sql
├── benchmarks/run_benchmark.py
├── data/{raw,lake,clean,processed}/
├── results/                 # CSV số đo, log, ảnh
├── docs/                    # data_contract.md, sơ đồ, kế hoạch
└── report/
```
Quy tắc: file nào do script sinh ra thì script đó nằm trong repo và được nhắc trong README.

## 6. Rủi ro
| Rủi ro | Cách giảm |
|---|---|
| Spark hết bộ nhớ trên 8 GB | Giảm số tháng, tăng partition, đọc cột cần thiết |
| Tải dữ liệu chậm hoặc lỗi mạng | Tải sớm (19/10), script hỗ trợ tải lại phần thiếu |
| Schema khác nhau giữa các tháng | Ép schema rõ ràng ở bước ingest |
| Báo cáo dồn cuối | Mỗi ngày viết 1 đoạn như cột bên phải bảng lịch |
| Đồ án có thể bị coi là sao chép pipeline có sẵn | Ghi lập luận chọn thiết kế và phụ lục dùng AI |
