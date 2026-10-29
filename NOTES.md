# NOTES – Đồ án Nhập môn Dữ liệu lớn

## 2026-10-18 – Bước 0: đọc yêu cầu & lập kế hoạch
- Đã đọc file yêu cầu (.docx): nhóm ≤5 người, chọn 1 quy trình con trong pipeline
  Sources → Ingestion → Storage → Processing → Analytics → Presentation → Decision/Action.
- Sản phẩm nộp: báo cáo PDF (~30 trang, cấu trúc mục 6.1), mã nguồn + README, dữ liệu mẫu/script sinh dữ liệu,
  minh chứng demo (log/ảnh/dashboard), bảng phân công, (khuyến nghị) phụ lục dùng AI.
- Thang điểm: hoàn thành hệ thống 2.5 | thiết kế & triển khai 2.0 | thực nghiệm & bằng chứng 2.0 |
  đặc trưng Big Data & scale 1.5 | phân tích & thảo luận 1.0 | chất lượng báo cáo & tái lập 1.0.
- Chưa rõ: nhóm nào, chọn quy trình nào, deadline, miền dữ liệu -> chờ người dùng xác nhận.
- Mâu thuẫn trong đề: tiêu đề mục 6 ghi "đánh giá giữa kỳ" nhưng nội dung ghi "báo cáo cuối kỳ" -> cần hỏi GV.

## 2026-10-18 – Bước 1: chốt hướng
- Người dùng chọn: Storage→Processing (PySpark/Spark SQL); dữ liệu công khai, không vướng bản quyền; deadline 2 tuần (~01/11/2026).
- Nguồn dữ liệu đề xuất: NYC TLC Trip Record Data (Parquet, theo tháng, URL dạng
  https://d37ci6vzurychx.cloudfront.net/trip-data/{loại}_tripdata_{YYYY-MM}.parquet; loại: yellow, green, fhv, fhvhv).
- Trang TLC không nêu giấy phép rõ ràng, chỉ có disclaimer về độ chính xác -> báo cáo phải trích dẫn nguồn + ghi rõ.
- Môi trường cloud của Claude KHÔNG tải được file từ cloudfront (proxy 403) -> script tải chạy trên máy người dùng.
- Việc kế tiếp (chờ người dùng duyệt): scaffold repo + docker-compose + scripts/download_data.py.

## 2026-10-18 – Bước 2: chốt dữ liệu, phần cứng, phân công
- Người dùng đồng ý: Yellow làm chính + vài tháng HVFHV test scale. Máy ~8 GB RAM. Nhờ chia việc (chưa nói số thành viên -> thiết kế cho 5, có bảng gộp vai trò cho 2–4 người).
- Đã tạo docs/PHAN_CONG.md (vai trò A–E, lịch 14 ngày, hợp đồng giữa các vai trò, cấu hình 8 GB).
- Điều chỉnh vì 8 GB: PySpark local[2], driver 3g, shuffle partitions 16; Docker chỉ dùng cho tái lập (giới hạn 4 GB); dữ liệu lõi 12 tháng Yellow, HVFHV 1–2 tháng.
- Chưa tạo script nào ở bước này (không có file do script sinh ra).
- Việc kế tiếp (chờ duyệt): scaffold repo + scripts/download_data.py + job kiểm tra Spark.

## 2026-10-18 (tối) – Bước 3: nhóm chỉ 1 người
- Người dùng làm 1 mình -> bỏ phân công 5 người (chuyển thành docs/archive_PHAN_CONG_5_nguoi.md), tạo docs/KE_HOACH_CA_NHAN.md.
- Phân tầng: BẮT BUỘC / NÊN CÓ / NẾU CÒN THỜI GIAN; lịch 14 ngày, mỗi ngày kèm 1 phần báo cáo; 2 ngày cuối làm dự phòng.
- Chưa tạo script nào. Việc kế tiếp (chờ người dùng nhắn "tiếp"): scaffold repo, scripts/download_data.py, job kiểm tra Spark.

## 2026-10-18 (tối) – Bước 4: dựng khung dự án (ngày 18/10 trong lịch)
- Tạo: requirements.txt (pyspark 3.5.3, pandas, pyarrow, matplotlib), .gitignore, README.md, thư mục data/ results/ sql/ benchmarks/ report/.
- Script mới (đều nằm trong repo): scripts/download_data.py, jobs/common.py, jobs/00_check_env.py.
- Đã kiểm thử trong môi trường cloud của Claude (Python 3.13, Java có sẵn):
  - 00_check_env.py chạy OK: 2 triệu dòng, ghi/đọc Parquet có partition, 265 nhóm, ~17 giây.
  - download_data.py: --dry-run in đúng URL; thử với server giả localhost: tải OK, bỏ qua file đã có, báo 404 khi thiếu, exit code 1 khi có lỗi.
  - CHƯA tải được dữ liệu thật (môi trường cloud bị chặn cloudfront) -> người dùng tự chạy trên máy.
  - URL bảng taxi_zone_lookup.csv (…/misc/taxi_zone_lookup.csv) chưa kiểm tra được từ đây; nếu 404 thì lấy link từ trang TLC và dùng --zones-only với --base-url hoặc tải tay vào data/raw/.
- File do script sinh ra: results/env_check.json (do 00_check_env.py), results/download_log.csv (do download_data.py) – đã xoá bản chạy thử của Claude để người dùng tạo bản trên máy mình.
- Việc kế tiếp (chờ duyệt): chạy 3 lệnh ở README trên máy người dùng, gửi kết quả; sau đó viết 01_ingest_to_lake.py.

## 2026-10-18 (tối) – Sửa lỗi Windows: "HADOOP_HOME and hadoop.home.dir are unset"
- Nguyên nhân: Spark trên Windows cần winutils.exe khi ghi file (lỗi xảy ra ở df.write.parquet trong 00_check_env.py; Java/PySpark đã chạy được).
- Sửa: thêm scripts/setup_windows_hadoop.py (tải winutils.exe + hadoop.dll từ cdarlint/winutils, thư mục hadoop-3.3.6, vào tools/hadoop/bin; in SHA-256);
  jobs/common.py tự đặt HADOOP_HOME/PATH trên Windows, báo lỗi hướng dẫn nếu thiếu; 00_check_env.py dùng đường dẫn file:/// và dọn thư mục tạm bỏ qua lỗi khoá file.
- Kiểm thử ở cloud (Linux): 00_check_env.py vẫn OK; setup script chạy đúng với server giả (tải, in hash, bỏ qua khi đã có); nhánh Windows thiếu winutils báo lỗi rõ.
- CHƯA kiểm thử trên Windows thật và chưa kiểm tra được URL GitHub thật từ cloud (repo có thư mục hadoop-3.3.6 theo trang GitHub; nhánh mặc định thử master rồi main).
- winutils là binary không chính thức -> ghi vào phụ lục báo cáo; phương án thay thế: WSL2/Docker.

## 2026-10-18 (tối) – Bước 5: nạp dữ liệu vào Data Lake
- Người dùng chạy OK trên Windows: setup_windows_hadoop.py (winutils tải được từ GitHub thật) và 00_check_env.py (Python 3.11.9, Java 1.8.0_401, PySpark 3.5.3, 11.27 s). Đã tải dữ liệu raw (chưa gửi kết quả inspect).
- Script mới: scripts/inspect_raw.py (pyarrow), jobs/01_ingest_to_lake.py, tests/make_fake_tlc.py (sinh dữ liệu giả – cũng là "script sinh dữ liệu mẫu" cho mục tái lập).
- Quyết định thiết kế ingest: đọc từng file tháng; tên cột về chữ thường; ép schema đích YELLOW_SCHEMA (cột thiếu -> null, ví dụ airport_fee/Airport_fee, cbd_congestion_fee); phân vùng year/month theo TÊN FILE;
  không làm sạch ở bước này; dynamic partition overwrite -> chạy lại idempotent; kiểm chứng rows_in (metadata pyarrow) vs rows_out (đọc lại lake); dim_zone từ taxi_zone_lookup.csv; đường dự phòng pyarrow khi Spark không đọc được timestamp ns.
- Kiểm thử bằng dữ liệu giả ở cloud (3 tháng x 200k dòng, gồm khác kiểu passenger_count, Airport_fee/airport_fee, timestamp ns): 600.000/600.000 dòng khớp, chạy lại 1 tháng vẫn đúng 600.000 tổng, đường dự phòng ns hoạt động.
- Lỗi đã gặp & sửa khi test: đọc lại phân vùng bằng URI chứa "year=2024" bị mã hoá %3D -> đổi sang đọc cả lake + lọc year/month.
- File do script sinh ra: results/schema_inspection.csv, raw_files_summary.csv (inspect_raw.py); results/ingest_log.csv, data/lake/** (01_ingest_to_lake.py); data/fake_raw/** (make_fake_tlc.py). Tất cả script đã nằm trong repo.
- CHƯA chạy trên dữ liệu thật/Windows. Việc kế tiếp: người dùng chạy inspect_raw.py + 01_ingest_to_lake.py, gửi kết quả; sau đó viết 02_clean.py dựa trên schema và lỗi thật.

## 2026-10-18 20:04 – Kết quả chạy thật trên máy người dùng (Windows, Python 3.11.9, Java 8)
- Dữ liệu thô: Yellow 2024-01..2024-12, 12 file, tổng 41.169.720 dòng, ~693 MB Parquet (50–64 MB/tháng; 2,96–3,83 triệu dòng/tháng; 3–4 row group/file).
- Schema thật: 19 cột, ĐỒNG NHẤT cả 12 tháng, không có cbd_congestion_fee (chỉ có từ 2025); cột là "Airport_fee" (A hoa); kiểu: VendorID int32, passenger_count int64, RatecodeID int64, store_and_fwd_flag large_string, PU/DOLocationID int32, payment_type int64, còn lại double; timestamp[us].
  -> logic ép schema của 01_ingest chưa cần dùng nhiều ở 2024 nhưng giữ cho 2025/HVFHV; cột cbd_congestion_fee toàn null ở 2024.
- 01_ingest_to_lake.py chạy OK: 12/12 tháng rows_in == rows_out (status OK). Mỗi tháng 3,7–7,7 s (tháng đầu chậm nhất do khởi động JVM); tổng ~57 s cho 41,2 triệu dòng (~720 nghìn dòng/giây, 2 core, 3g).
- Lake ~901 MB (65–84 MB/tháng), lớn hơn raw ~30%: có thêm cột source_file/year/month và nhiều khả năng khác codec nén (chưa kiểm tra) -> kiểm tra trước khi viết vào báo cáo.
- Chưa gửi/nhận: results/ingest_log.csv bản đầy đủ (chỉ có console output).
- Việc kế tiếp (chờ người dùng nhắn "tiếp"): jobs/02a_profile_raw.py – đo lỗi dữ liệu thật (null, ngày ngoài tháng, âm, khoảng cách 0, thời gian âm/quá dài, PU/DO ngoài 1..265...) để chốt luật làm sạch dựa trên số liệu, rồi 02_clean.py.

## 2026-10-18 20:1x – Bước 6: profiling chất lượng dữ liệu (jobs/02a_profile_raw.py)
- Mục đích: đo lỗi thật trước khi chốt luật làm sạch (không đoán ngưỡng). Một lượt groupBy(year, month) tính ~35 luật + null từng cột; thêm phân vị (approxQuantile), tần suất cột phân loại, tuỳ chọn --duplicates theo tháng.
- Luật gồm: giờ null, thời lượng âm/0/<1 phút/>3h/>24h, ngày đón ngoài tháng/năm, quãng đường âm/0/>100, tốc độ >100 mph, fare/total âm/0/lớn, tip/tolls/extra/mta âm, passenger null/0/>6, PU/DO null/ngoài bảng khu vực/Unknown(264,265), payment_type, ratecodeid.
- Test trên dữ liệu giả (3 x 200k dòng, lỗi chèn 1% mỗi loại): đếm đúng 6.000/600.000 cho thời lượng âm, ngày 2002, passenger null, fare âm.
- Lỗi khi test: DataFrame.toPandas() của PySpark 3.5 hỏng trên Python 3.13 (thiếu distutils) -> đổi sang collect() + pd.DataFrame (chạy được mọi phiên bản; người dùng dùng Python 3.11).
- Chưa chạy trên dữ liệu thật. File sinh ra: results/dq_*.csv (do 02a_profile_raw.py; script nằm trong repo).
- Việc kế tiếp: người dùng chạy script, gửi dq_profile.csv, dq_nulls.csv, dq_quantiles.csv, dq_categorical.csv (+ dq_duplicates.csv); rồi viết 02_clean.py theo số liệu thật.

## 2026-10-18 20:2x – Tách thư mục kết quả theo từng bước (theo yêu cầu người dùng)
- Quy ước: mỗi script ghi vào results/<tên script bỏ .py>/ (download_data, 00_check_env, inspect_raw, 01_ingest_to_lake, 02a_profile_raw; các bước sau tương tự: 02_clean, 03_aggregate, run_benchmark).
- Sửa: jobs/common.py thêm results_dir(); 00_check_env, 01_ingest_to_lake, 02a_profile_raw, inspect_raw, download_data dùng thư mục mới (tham số --results vẫn ghi đè được).
- Script mới: scripts/reorganize_results.py (chuyển file cũ nằm phẳng vào thư mục đúng; idempotent; có --dry-run). Người dùng đã có results/ingest_log.csv, schema_inspection.csv, raw_files_summary.csv, env_check.json (và download_log.csv) nằm phẳng -> chạy script này một lần.
- Kiểm thử ở cloud: reorganize (dry-run, chạy thật, chạy lần 2 = 0 file), cả 5 script ghi đúng thư mục mặc định.

## 2026-10-18 20:4x – Bước 7: kết quả profiling thật + làm sạch (jobs/02_clean.py)
### Số liệu thật (results/02a_profile_raw/, 41.169.720 dòng)
- Lỗi thời gian: giờ trả < giờ đón 1.575; thời lượng = 0: 11.935; 1–59 giây: 488.060 (1,19%); > 3 giờ: 25.349; > 24 giờ: 230; ngày đón ngoài tháng: 420 (ngoài năm: 56).
- Quãng đường: = 0: 776.305 (1,89%); > 100 dặm: 1.613; tốc độ TB > 100 mph: 12.171. Tiền: fare < 0: 731.024 (1,78%); total < 0: 609.344; mta_tax < 0: 583.164; extra < 0: 304.569; fare = 0: 17.260; fare > 500: 600; total > 1000: 43.
- Null: passenger_count, ratecodeid, store_and_fwd_flag, congestion_surcharge, airport_fee cùng null đúng 4.091.232 dòng (9,94%) và TRÙNG với payment_type = 0 (4.091.232), từng tháng cũng khớp -> cùng một tập dòng (loại thanh toán "0"); cbd_congestion_fee null 100% (chưa có ở 2024).
- passenger_count = 0: 401.354 (0,97%); > 6: 284 (7,8,9). vendorid có thêm 6 (2.069) và 7 (230) bên cạnh 1, 2. Dòng trùng (khoá 8 cột): chỉ 5 dòng/12 tháng.
- Mã khu vực: không dòng nào ngoài bảng dim_zone; thuộc nhóm Unknown (264/265): PU 146.201, DO 370.258.
- Phân vị gốc: fare p50 13,5, p99 80; distance p99 20; duration p99 4.229 s; max fare 335.544; max distance 398.609 dặm.
- LƯU Ý: p99.9 trong dq_quantiles.csv trùng với max (sai số approxQuantile 0,001 trên 41 triệu dòng quá lỏng) -> không dùng p99.9 gốc trong báo cáo; 02_clean tính lại phân vị SAU làm sạch với sai số 1e-5.
### Quyết định làm sạch (có tham số)
- LOẠI vào data/clean/yellow_rejected (kèm reject_reasons): thời lượng <= 0 hoặc > 24h; ngày đón ngoài tháng; trip_distance <= 0 hoặc > 100 dặm; tốc độ TB > 100 mph; fare <= 0 hoặc > 500; total <= 0 hoặc > 1000; bất kỳ phụ phí/tip/thuế < 0; mã khu vực null/không có trong dim_zone. Thời lượng 1–59 giây KHÔNG loại (488k dòng, chưa có căn cứ).
- SỬA: passenger_count = 0 hoặc > 6 -> null, cột cờ passenger_count_repaired (lỗi nhỏ, phần còn lại tốt).
- GIỮ: dòng payment_type = 0 (null cùng bộ cột) vì là chuyến thật; thêm cột is_flex_fare. Unknown 264/265 giữ nguyên (xử lý ở bước KPI). Vendor 6, 7 giữ.
- Loại dòng trùng hệt theo toàn bộ cột nghiệp vụ trong cùng tháng.
- Cột thêm: trip_duration_s, avg_speed_mph, pickup_hour, pickup_dow (1=CN..7=T7), is_flex_fare, passenger_count_repaired.
- GIẢ THUYẾT CHƯA KIỂM CHỨNG: các khoản tiền âm có thể là hoàn tiền/tranh chấp (payment_type 4 = 794.494 dòng, 3 = 291.743) – chưa có bảng chéo; nên kiểm tra bằng groupBy(payment_type) trên yellow_rejected trước khi viết vào báo cáo. Flex Fare = payment_type 0 theo từ điển TLC cần đối chiếu trong data dictionary trước khi khẳng định.
### Kiểm thử ở cloud (dữ liệu giả 2 x 201.000 dòng, chèn lỗi + trùng + passenger 0/9)
- Bảo toàn số dòng mỗi tháng (vào = trùng + loại + đạt) OK; loại trùng 1.000/tháng; passenger đạt chỉ còn 1–6; reject_reasons nối nhiều mã bằng "|".
- tests/make_fake_tlc.py cập nhật (thêm passenger 0/9 và dòng trùng). Chưa chạy trên dữ liệu thật; chưa đo thời gian 41 triệu dòng (dữ liệu giả: ~7–12 s/tháng với 200k dòng, không ngoại suy được).
- File do script sinh ra: results/02_clean/* và data/clean/** (jobs/02_clean.py, nằm trong repo).
- Việc kế tiếp: người dùng chạy 02_clean.py và gửi results/02_clean/*.csv (+ log lỗi nếu có); sau đó jobs/03_aggregate.py + sql/kpi_*.sql.

## 2026-10-18 20:5x – Giải thích lý do làm sạch (theo yêu cầu người dùng)
- docs/GIAI_THICH_LAM_SACH.md – nguyên tắc, thứ tự xử lý, từng luật (bằng chứng/lý do/rủi ro), phần phán đoán vs chưa kiểm chứng, câu hỏi vấn đáp. Dùng cho mục "Xử lý dữ liệu/Thảo luận" của báo cáo.
- Việc đề xuất làm trước khi viết báo cáo: bảng chéo payment_type x reject_reasons; phân tích độ nhạy với 2–3 bộ ngưỡng; đối chiếu data dictionary TLC.

## 2026-10-19 – Giải thích lý do làm sạch (theo yêu cầu người dùng)
- Viết docs/GIAI_THICH_LAM_SACH.md: 5 nguyên tắc, lý do + số liệu từng luật, điểm yếu, ánh xạ vào code, 5 kiểm tra cần làm, câu trả lời mẫu khi bảo vệ.
- Điểm mình tự nhận là yếu/chưa kiểm chứng: (1) tiền âm = hoàn tiền (chưa có bảng chéo payment_type); (2) payment_type 0 = Flex Fare (cần đối chiếu data dictionary); (3) loại dist = 0 (1,89%) có thể bỏ sót chuyến thật; (4) ngưỡng 24h/100 dặm/100 mph/500 USD là phán đoán, cần chạy độ nhạy; (5) loại cả hoàn tiền lẫn chuyến gốc chưa chắc triệt tiêu.
- Đề xuất bước sau (chờ người dùng quyết): jobs/02b_verify_rules.py gồm bảng chéo + độ nhạy ngưỡng.

## 2026-10-19 18:5x – KẾT QUẢ CHẠY THẬT 02_clean.py (Windows, 2 core, 3g)
- 12/12 tháng status OK (bảo toàn: rows_in = trùng + loại + đạt). Tổng vào 41.169.720 | trùng 4 | loại 1.478.322 (3,59%) | ĐẠT 39.691.394 (96,41%) | sửa passenger_count 401.638 (0,98%).
- Tỉ lệ loại theo tháng 2,88% (T4) – 4,14% (T9); T9 4,14%, T12 4,11%, T3 4,02%, T10 3,99% cao nhất. Trong profile: fare âm tăng gần gấp đôi trong năm (T1 37.448 -> T12 78.444); quãng đường 0 đột biến T3 (87k), T9/T10 (81k) so với T4 (46k).
- Thời gian: 25,9–46,8 s/tháng, tổng ~494 s (~8,2 phút) ≈ 83 nghìn dòng/giây; chậm hơn ~9 lần so với bước nạp (~720 nghìn dòng/giây) do dropDuplicates (shuffle) + persist + 2 lần ghi + đếm kiểm chứng.
- Đối chiếu với profile: DUR_NONPOSITIVE 13.510 = 1.575 + 11.935; FARE_NONPOSITIVE 748.284 = 731.024 + 17.260; TOTAL_NONPOSITIVE 614.406 = 609.344 + 5.062; SPEED 12.171, DIST_GT 1.613, FARE_GT 600, TOTAL_GT 43 khớp tuyệt đối. DIST_NONPOSITIVE 776.303 so với 776.305 (lệch 2 do bỏ 4 dòng trùng). passenger_repaired 401.638 = 401.354 + 284 khớp.
- Dòng trùng: khoá 8 cột (profile) = 5 (T2:1, T7:1, T9:1, T10:2); toàn bộ cột = 4 (T9 = 0) -> cặp ở T9 chỉ trùng khoá, khác ở cột khác = chuyến khác nhau, xác nhận việc dùng toàn bộ cột là đúng.
- Trước/sau (min/max chính xác): fare −2.261,20..335.544,44 -> 0,01..500; quãng đường max 398.608,62 -> 99,86; thời lượng −85.623 s..586.051 s -> 1..86.399 s; total −2.265,45..335.550,94 -> 0,01..783,47. Phân vị sau làm sạch (sai số 1e-5, p99.9 đã có ý nghĩa): fare p50 14,2 / p99 77,2 / p99.9 143,7; distance p50 1,8 / p99 20,06 / p99.9 29,68; duration p50 786 s / p99 4.262 / p99.9 7.209; speed p50 9,28 / p99 34,58 / p99.9 45,78 mph.
- ĐIỀU ĐÁNG NGỜ SAU KẾT QUẢ:
  (1) Tổng hai nhóm lớn (dist <= 0: 776.303; fare <= 0: 748.284) = 1.524.587 nhưng tổng dòng bị loại chỉ 1.478.322 -> hai nhóm giao nhau ít nhất 46.265 dòng; nếu các luật tiền khác nằm trong nhóm fare <= 0 (chưa kiểm chứng) thì phần lớn dòng quãng đường 0 có fare > 0 => KHÔNG phải hoàn tiền/huỷ, mà có thể là lỗi ghi quãng đường (đột biến theo tháng gợi ý lỗi theo nhà cung cấp). Giả định "quãng đường 0 = khách huỷ" yếu đi; loại chúng có thể làm mất ~1,7% dòng có doanh thu.
  (2) Ngưỡng tốc độ 100 mph lỏng: p99.9 chỉ 45,8 mph mà max đúng 100.
  (3) Sau làm sạch vẫn còn fare 0,01–1,0 (p0.1 = 1,0), thấp hơn phí khởi điểm taxi (nhớ là 3 USD, cần đối chiếu quy định TLC) và chuyến 1–12 giây (p0.1 = 12 s).
- Việc kế tiếp đề xuất: jobs/02b_verify_rules.py (bảng chéo dist=0 x fare/payment_type/vendor/tháng; tiền âm x payment_type; đếm fare < 3, speed > 60/80, duration < 30 s; độ nhạy ngưỡng) rồi quyết định giữ/đổi luật dist <= 0 trước khi qua 03_aggregate.

## 2026-10-19 23:3x – Viết jobs/02b_verify_rules.py
- Đối chiếu data dictionary TLC (WebFetch): payment_type 0 = Flex Fare; vendor 1 Creative Mobile, 2 Curb Mobility, 6 Myle, 7 Helix; dictionary KHÔNG nói gì về giá âm/hoàn tiền. Trang giá cước TLC: phí khởi điểm 3,00 USD, 0,70 USD/phút khi xe chậm/đứng, JFK đồng giá 70 USD (trang không ghi ngày hiệu lực -> giả định cho 2024).
- Script trả lời từng điểm đáng ngờ: V0 tác động RIÊNG từng luật (chồng lấn), V1 tiền âm x payment_type (giả thuyết hoàn tiền), V2/V2b/V2c quãng đường 0 (theo tháng, vendor, thanh toán, thời lượng, giá có giải thích được bằng phí khởi điểm + phí thời gian không), V3 Flex Fare = null 5 cột?, V4 ứng viên luật mới trên dữ liệu đạt (giá < 3, tốc độ > 40/60/80, chuyến < 10/30/60 s, > 3h/6h), V5 độ nhạy KPI (%) theo 6 kịch bản, V6 loại theo tháng x luật, V7 nhất quán nghiệp vụ (tổng tiền, tip tiền mặt, giá JFK, phí sân bay).
- Mới chạy thử trên dữ liệu giả (chạy được, ~0,2 phút); chưa chạy dữ liệu thật; số liệu giả không có ý nghĩa kết luận. Một vài kiểm V7 trên dữ liệu giả báo vi phạm lớn vì dữ liệu giả không mô phỏng cấu trúc phí – chỉ đánh giá trên dữ liệu thật.
- File sinh ra: results/02b_verify_rules/*.csv (jobs/02b_verify_rules.py).
- Việc kế tiếp: người dùng chạy và gửi results/02b_verify_rules/*.csv -> quyết định giữ/đổi luật quãng đường 0, siết tốc độ/giá; chạy lại 02_clean (~8 phút) nếu đổi; rồi 03_aggregate.

## 2026-10-20 – KẾT QUẢ THẬT 02b_verify_rules.py (người dùng chạy trên 41,17 triệu dòng)
- V3 Flex Fare: payment_type=0 (4.091.232 dòng) <=> null đủ 5 cột (passenger_count, ratecodeid, store_and_fwd_flag, congestion_surcharge, airport_fee): tương đương TUYỆT ĐỐI hai chiều (mọi dòng non-Flex không null cột nào trong 5). Tổng 41.169.720 khớp.
- V1 tiền âm: fare<0 = 731.024 dòng; 81,5% (595.518) "đảo toàn bộ" (mọi khoản <= 0, total < 0), chủ yếu Dispute 362.842 (45,7% dòng Dispute), Cash 142.529, No charge 89.911 (30,8%); Credit card chỉ 239/30,4 triệu. => giả thuyết "bản ghi hoàn tiền/điều chỉnh" được ủng hộ cho nhóm này (CHƯA chứng minh triệt tiêu với chuyến gốc). Ngoại lệ: Flex Fare fare<0 nhưng không đảo toàn bộ = 127.089 dòng (3,1% Flex), tương ứng 125.814 dòng chỉ bị luật FARE_NONPOSITIVE (V0) -> KHÔNG giống hoàn tiền (chưa rõ), cần 02c.
- V0: TOTAL_NONPOSITIVE chỉ riêng nó 25 dòng, NEG_COMPONENT riêng 113 dòng -> hai luật thừa (nằm gần như trọn trong FARE_NONPOSITIVE); giữ cũng không hại. DIST_NONPOSITIVE: 703.596/776.303 (90,6%) chỉ bị riêng luật này.
- V2/V2b/V2c quãng đường 0 (776.303): KHÔNG đồng nhất. 53,5% giá cao hơn mức phí thời gian giải thích (giá TB 42,4 USD, 414.897 dòng); 17,5% chỉ phí khởi điểm (~2,9 USD: giống chuyến huỷ); 16,4% giá = khởi điểm + phí thời gian (xe đứng yên); 7,9% giá <= 0. Theo ratecode: ratecode 5 (thương lượng) 19,0%, null=Flex 35,1%, ratecode 99 3,5%; pu==do 48,1% với thời lượng TB 93 s; 42,7% chuyến < 1 phút. Theo vendor: Creative Mobile 4,6%/5,8%/6,7% ở T1–T3 rồi ~1,9–2,7% (lỗi/đột biến theo nhà cung cấp ở Q1); Curb Mobility tăng 1,1% -> 2,0–2,2% từ T8–T9. Kết luận: là hỗn hợp (chuyến huỷ + đồng hồ không ghi quãng đường ở Flex/giá thương lượng + lỗi thiết bị), không phải "hoàn tiền".
- V5 độ nhạy (so baseline): B giữ lại 703.596 dòng quãng đường 0: dòng +1,77%, doanh thu +2,11%, quãng đường TB -1,74%, total TB +0,33% -> KHÔNG bền vững cho doanh thu/quãng đường. C speed<=60: dòng -0,008%. D fare>=3: dòng -0,24%, doanh thu -0,045%. E duration>=30s: dòng -0,17%, doanh thu -0,33%. F duration<=6h: dòng -0,054% nhưng thời lượng TB -3,89% (21.379 dòng >6h, TB 21 giờ, tốc độ 0,3 mph = đồng hồ quên tắt). G chặt: dòng -0,48%, doanh thu -0,50%, thời lượng TB -3,77%.
- V4: fare<3 với quãng đường>0: 95.626 dòng (0,24%), giá TB 1,24 USD cho 3,4 dặm/19 phút -> không hợp lệ; speed>60: 3.228 dòng (TB 75 mph), >80: 1.024; thời lượng <10 s: 30.708 dòng nhưng giá TB 57,5 USD, 0,03 dặm (kiểu Flex/thương lượng); quãng đường > 30 dặm 37.431 dòng, giá TB 180 USD, tốc độ 35 mph (chuyến xa thật, giữ).
- V6: loại do fare<=0 tăng đều 1,29% (T1) -> 2,19% (T12); DUR_NONPOSITIVE T11 đột biến 0,051%; các luật khác ổn định.
- V7 (trên dữ liệu ĐẠT): tip tiền mặt > 0: 268/5,28 triệu (0,005%) khớp dictionary; ratecode 2 (JFK) fare != 70: 268/1,33 triệu (0,02%) -> đồng giá 70 USD ĐƯỢC XÁC NHẬN cho 2024 (số 268 trùng số ở dòng trên là trùng hợp? cần soát lại khi chạy 02c); airport_fee>0 mà điểm đón không phải JFK/LGA: 136.540/3,17 triệu (4,3%) chưa giải thích; Flex có tip>0: 629.901 dòng (thông tin, không phải vi phạm).
- ĐIỂM MỚI ĐÁNG NGỜ NHẤT: total_amount khác tổng các khoản ở 11.348.614 dòng ĐẠT (28,6%) -> nếu total_amount không đáng tin thì KPI doanh thu bị ảnh hưởng. Chưa có lời giải thích từ dữ liệu -> viết jobs/02c_diagnose_open_items.py (D1 giá trị chênh lệch + theo nhóm; D2 hồ sơ nhóm fare<0 total>0). Mới chạy dữ liệu giả.
- File sinh ra: results/02b_verify_rules/*.csv (jobs/02b_verify_rules.py); results/02c_diagnose_open_items/* (jobs/02c_diagnose_open_items.py).
- Đề xuất đổi luật (chờ người dùng đồng ý, sau 02c): (a) dist<=0 chuyển từ LOẠI sang GIỮ + cờ is_zero_distance, đặt trip_distance=null và tốc độ=null; (b) max-duration-h 24 -> 6; (c) thêm FARE_BELOW_MIN: fare < 3 và quãng đường > 0; (d) max-speed 100 -> 80 (tác động < 0,02%, tuỳ chọn); (e) giữ nguyên các luật còn lại.

## 2026-10-20 – KẾT QUẢ THẬT 02c_diagnose_open_items.py + ĐỔI LUẬT 02_clean.py (phiên bản 2)
- D1 total_amount lệch tổng các khoản (11,35 triệu dòng đạt, 28,6%): KHÔNG ngẫu nhiên. Chênh lệch (total − tổng các khoản) chỉ gồm vài giá trị cố định: −2,50 (65,9%, 7,48 triệu dòng), +2,50 (29,2%, 3,32 triệu), −4,25 (2,8%) và −1,75 (2,0%) ≈ 99,9% số dòng lệch. Giải thích: 2,50 = phụ phí tắc nghẽn NYS, 1,75 = airport_fee, 4,25 = cả hai. (a) +2,5: dòng Flex Fare (cột congestion_surcharge null, lệch 88,3% Flex) – total đã gồm 2,50 nhưng cột để null; (b) −2,5/−1,75/−4,25: cột phụ phí có giá trị nhưng total KHÔNG cộng (Creative Mobile lệch 92,5%, Curb Mobility 8,7%); chưa tách được −2,5 theo nhà cung cấp x Flex nên "tập trung ở Creative Mobile" là suy luận. Mức độ: tổng chênh ròng ≈ −12,16 triệu USD ≈ 1,07% doanh thu baseline (tự tính từ bảng giá trị chênh; số gần đúng) -> quy ước ghi total_amount khác nhau theo nhà cung cấp, không phải nhiễu. Quyết định: KPI doanh thu dùng total_amount "như TLC báo cáo" (không tự cộng lại), ghi rõ giới hạn ~1% trong báo cáo; phân tích theo từng phụ phí phải tránh Flex và cẩn thận với Creative Mobile.
- D2 125.814 dòng chỉ bị FARE_NONPOSITIVE: 97,2% Flex Fare, 100% total > 0 và quãng đường > 0; fare trung vị đúng −1,00 USD, total trung vị 3,00 USD (= 0,5 mta + 1,0 improvement + 2,5 congestion − 1,0), quãng đường TB 2,5 dặm/852 s -> đây là chuyến THẬT nhưng cột tiền không dùng được (total 3 USD cho 13 phút/1,9 dặm), KHÔNG phải hoàn tiền. Giữ trong yellow_rejected (không tự gán giá). 2,1–3,5% dòng Flex mỗi tháng (ổn định ~3%, T1 1,6%).
- Phát hiện thêm: số dòng Flex Fare tăng mạnh trong năm (T1 140.162 ≈ 4,7% -> T9 483.731 ≈ 13,3% số dòng) – thay đổi cơ cấu; giải thích một phần vì sao các tỉ lệ lỗi theo tháng (quãng đường 0 ở Curb Mobility, fare<=0) tăng dần. Cần nêu trong phần thảo luận và tách Flex khi so sánh theo tháng.
- ĐỔI LUẬT trong jobs/02_clean.py (bản 2, đã chạy thử dữ liệu giả, CHƯA chạy dữ liệu thật):
  (1) DIST_NONPOSITIVE bỏ khỏi luật LOẠI: trip_distance = 0 -> SỬA: trip_distance=null, avg_speed_mph=null, cột cờ is_zero_distance (bản ghi giữ nguyên giá/thời lượng). Chỉ còn DIST_NEGATIVE (dist < 0) là luật loại. Lý do: 02b V5 – giữ 703.596 dòng này đổi doanh thu +2,11% và quãng đường TB −1,74%; V2: hỗn hợp chuyến huỷ/Flex/giá thương lượng/lỗi thiết bị, giá bình thường.
  (2) --max-duration-h 24 -> 6 (21.379 dòng > 6 giờ có TB 21 giờ, 0,3 mph; thời lượng TB đổi 3,9%).
  (3) --max-speed 100 -> 80 (tác động < 0,02%).
  (4) Thêm FARE_BELOW_MIN: 0 < fare < 3 USD khi quãng đường > 0 (--min-fare 3; 95.626 dòng; giá TB 1,24 USD cho 3,4 dặm).
  (5) Đếm số vi phạm từng luật bằng cột reject_reasons (vì trip_distance/passenger_count đã bị sửa trước khi đếm); thêm cột zero_distance_flagged vào clean_summary_by_month.csv và dòng ZERO_DISTANCE_FLAGGED trong rule_counts.csv.
- Lưu ý: 02b_verify_rules.py đọc yellow_rejected theo mã DIST_NONPOSITIVE của bản 1; sau khi chạy lại 02_clean bản 2 các bảng V2 (quãng đường 0) sẽ rỗng – kết quả 02b đã nộp là bằng chứng cho quyết định. Đừng chạy lại 02b nếu không cập nhật.
- Việc kế tiếp: người dùng chạy lại python jobs/02_clean.py (~8 phút, ghi đè data/clean) và gửi 4 file results/02_clean/; sau đó viết jobs/03_aggregate.py + sql/kpi_*.sql.

## 2026-10-20 – Tách bản 2 thành file riêng (theo yêu cầu người dùng)
- jobs/02_clean.py khôi phục NGUYÊN BẢN 1 (đúng như đã chạy thật). Luật mới nằm ở jobs/02_clean_v2.py (mặc định ghi data/clean_v2 và results/02_clean_v2/, không đè bản 1). Các mô tả "02_clean.py bản 2" ở mục ngay trên đọc là 02_clean_v2.py.
- 02b/02c mặc định đọc data/clean (bản 1) nên vẫn chạy lại được. 03_aggregate sẽ đọc data/clean_v2.
- Việc kế tiếp: người dùng chạy python jobs/02_clean_v2.py (~8 phút) và gửi 4 file results/02_clean_v2/.

## 2026-10-20 10:xx – KẾT QUẢ THẬT 02_clean_v2.py và viết 03_aggregate.py
- 02_clean_v2 (6 giờ, 100 dặm, 80 mph, fare tối thiểu 3 USD): 12/12 tháng OK (bảo toàn). Vào 41.169.720 | trùng 4 | loại 892.954 (2,17%) | ĐẠT 40.276.762 (97,83%) – so với bản 1: loại 1.478.322 (3,59%), đạt 39.691.394 (96,41%). Cờ quãng đường 0: 776.303 (không đổi, tính cả dòng bị luật khác loại); xấp xỉ 703,5 nghìn nằm trong dữ liệu đạt (suy ra, chưa đếm trực tiếp).
- Luật mới: DUR_GT_MAX 22.023 (bản 1: 230, +21.793), SPEED_GT_MAX 13.245 (+1.074), FARE_BELOW_MIN 96.072 (0,23%). Phần chênh ròng 585.368 = 703.596 (quãng đường 0 giữ lại) − 118.228 (luật mới loại thêm).
- Phân vị sau làm sạch: duration max 21.597 s (≈ 6 giờ), speed max 80, fare p0.1 = 3,0; fare/total còn giá trị nhỏ nhất 0,01/0,02 = các dòng quãng đường 0 (luật fare tối thiểu chỉ áp khi quãng đường > 0) – lọc bằng is_zero_distance nếu cần. Duration p1 123 s -> 62 s vì các chuyến ngắn quãng đường 0 được giữ lại. fare/distance p50, p99 gần như không đổi (14,2 USD; 1,8 dặm).
- Thời gian: 24,4–30,1 s/tháng, tổng ≈ 333 s (≈ 5,6 phút), nhanh hơn bản 1 (494 s); chưa kiểm tra nguyên nhân (không chứng minh được do đếm theo reject_reasons).
- 03_aggregate.py + sql/kpi_daily, kpi_hourly_dow, kpi_zone_pickup, kpi_od_top, kpi_payment_month, kpi_vendor_dq: Spark SQL trên view trips (data/clean_v2) và zones (dim_zone); ghi Parquet data/processed/<kpi>/ và CSV results/03_aggregate/; kiểm tổng chuyến = số dòng sạch cho 5 KPI phân hoạch. Quy ước: doanh thu = sum(total_amount) như TLC báo cáo; trung bình quãng đường/tốc độ bỏ dòng quãng đường 0 (null); tip% chỉ chuyến thẻ; zone 264/265 giữ nhãn Unknown trong kpi_zone_pickup, bỏ khỏi kpi_od_top.
- Mới chạy thử trên dữ liệu giả (6 truy vấn, ~11 s); chưa chạy dữ liệu thật; chưa đo thời gian thật.
- File sinh ra: data/processed/*, results/03_aggregate/* (jobs/03_aggregate.py + sql/*.sql).
- Việc kế tiếp: người dùng chạy python jobs/03_aggregate.py, gửi results/03_aggregate/*.csv (aggregate_log.csv, kpi_*.csv); mình soát số liệu hợp lý rồi làm benchmark (bước 4).

## 2026-10-20 10:3x – KẾT QUẢ THẬT 03_aggregate.py, soát số liệu và viết 04_benchmark.py
- Chạy thật: 6 KPI đều OK; tổng chuyến mọi KPI phân hoạch = 40.276.762 (= số dòng sạch). Thời gian 2,0–5,7 s/KPI, tổng ≈ 19 s (đọc Parquet 40 triệu dòng rất nhanh nhờ chỉ đọc cột cần).
- Số liệu hợp lý: 366 ngày (2024 nhuận); ngày lễ thấp đúng (4/7 60.194; Lễ Tạ ơn 28/11 75.024; Giáng sinh 25/12 52.863; Memorial Day 25–27/5 giảm); đỉnh 17–18h ngày thường, đỉnh khuya T7/CN; tốc độ giữa ngày 8–10 mph; JFK->Times Sq fare TB 70,6 USD (đồng giá 70); Unknown 264 = 119.326 chuyến (0,30%), Outside NYC 265 = 21.236 (fare TB 92,3); JFK chiếm 4,7% chuyến nhưng 13,5% doanh thu (156,5 / ≈1.160 triệu USD).
- Xác nhận giả thuyết 02c bằng kpi_vendor_dq: tỉ lệ total_amount lệch tổng các khoản của Creative Mobile ≈ 92% mọi tháng, còn Curb Mobility 3,8% (T1) -> 10–12% (T3–T6, T9) ≈ 0,9 x tỉ lệ Flex Fare của chính hãng đó -> lệch của Curb Mobility gần như chỉ là dòng Flex (total có 2,50 nhưng cột null); lệch −2,50/−1,75 là đặc thù Creative Mobile (cột phụ phí có giá trị nhưng total không cộng).
- Quãng đường 0 theo ngày: Creative Mobile hỏng ở Q1 và hết đột ngột quanh 20/3/2024 (zero_distance_pct toàn hệ thống 2,3–4,1% -> ~1,2% từ 20/3); T9–T10 phần trăm cao hơn vào ngày thường (2–3,4%) so với cuối tuần (~1,2%) – chưa rõ nguyên nhân.
- ĐIỂM CẦN SỬA: avg_tip_pct_card (trung bình tỉ lệ tip/fare từng chuyến) nhảy vọt vài ngày (29/1: 89,5%; 15/4: 45,1%; 29/11: 46,7%; 16/9: 42,8% so với ~25%) -> trung bình tỉ lệ nhạy ngoại lệ (tip lớn trên fare nhỏ), chưa rõ nguyên nhân cụ thể. Đã sửa sql/kpi_daily.sql: tip_pct_card = tổng tip / tổng fare chuyến thẻ (bền), giữ mean_tip_ratio_card_pct để đối chiếu, thêm card_tip_gt_fare_rows (số chuyến thẻ có tip > fare). Chạy lại: python jobs/03_aggregate.py --only kpi_daily (≈6 s).
- Cần lưu ý trong báo cáo: (a) JFK->JFK 83.871 chuyến, fare TB 37,8 USD, 2,5 dặm/5,5 phút: nhiều khả năng lẫn chuyến đồng giá 70 USD bị ghi trả khách tại JFK (chưa kiểm chứng) -> nên tách/ghi chú cặp pu = do khi xếp hạng; (b) Flex Fare tip TB dao động 0,55–1,58 USD giữa các tháng (không ổn định); (c) vendor 7 (Helix, 230 dòng) KHÔNG xuất hiện sau làm sạch -> toàn bộ bị loại, chưa kiểm tra lý do; (d) Myle (vendor 6) từ T9 chỉ vài dòng và fare TB ~5,8 USD (rất ít, bỏ qua); (e) zone 56 và 57 cùng tên "Corona" (do bảng TLC); (f) doanh thu dùng total_amount nên chịu sai lệch ~1% đã nêu ở 02c.
- 04_benchmark.py: 5 thí nghiệm – scale (1/3/6/12 tháng; Spark so với Pandas 1/3/6 tháng), shuffle partitions (2/8/16/64/200, tắt AQE; thêm 1 dòng AQE bật), format (Parquet so với CSV 3 tháng: thời gian + dung lượng + thời gian tạo CSV), cores (local[1/2/4…]), pruning (lọc cột phân vùng so với lọc bằng hàm). Mỗi cấu hình 1 lượt khởi động + 3 lượt đo, báo median/min/max và dòng/giây; lượt khởi động còn tính tổng kiểm chứng (số chuyến, doanh thu) để chứng minh Spark và Pandas ra cùng kết quả. Đã chạy thử trên dữ liệu giả (kết quả không có ý nghĩa vì quá nhỏ), CHƯA chạy dữ liệu thật; chưa có số đo thời gian thật.
- File sinh ra: results/04_benchmark/* (jobs/04_benchmark.py), data/bench/* (CSV tạm cho thí nghiệm format, cũng do jobs/04_benchmark.py tạo).
- Việc kế tiếp: người dùng chạy python jobs/04_benchmark.py --pause (chụp Spark UI ở lần dừng) và gửi results/04_benchmark/*; chạy lại python jobs/03_aggregate.py --only kpi_daily và gửi kpi_daily.csv mới.

## 2026-10-20 10:4x – Chụp Spark UI tự động (theo yêu cầu người dùng: chụp tay dễ sót)
- Thêm jobs/ui_capture.py và cờ --capture-ui (+ --no-screenshots) cho jobs/04_benchmark.py. Sau MỖI thí nghiệm, khi Spark còn sống, lưu vào results/04_benchmark/spark_ui/<thí nghiệm>/: (1) JSON gốc + CSV tóm tắt từ REST API của Spark (jobs, stages gồm input/shuffle bytes và executorRunTime, sql, executors, environment) – không cần cài thêm; (2) ảnh PNG full-page các tab Jobs/Stages/Storage/Environment/Executors/SQL, từng trang chi tiết truy vấn SQL (sơ đồ DAG + số liệu từng toán tử) và 5 stage chạy lâu nhất – cần Playwright (pip install playwright; python -m playwright install chromium). Chưa cài Playwright thì chỉ bỏ ảnh.
- Đã kiểm thử trên dữ liệu giả ở môi trường cloud (JSON/CSV + 16 ảnh, ảnh sơ đồ truy vấn đọc được); CHƯA thử trên Windows của người dùng. Biến môi trường CHROMIUM_PATH cho phép dùng Chrome có sẵn nếu tải Chromium lỗi.
- File sinh ra: results/04_benchmark/spark_ui/** (jobs/ui_capture.py, gọi từ jobs/04_benchmark.py).
- Sửa ui_capture (10:5x): ảnh chi tiết truy vấn SQL/stage chỉ chụp cho cái MỚI kể từ lần capture trước (trước đó mỗi thư mục chụp lại 25 truy vấn đầu). Các tab tổng (Jobs/Stages/SQL...) và JSON/CSV vẫn tích luỹ cả phiên Spark, nên thư mục cuối của một phiên chứa toàn bộ số liệu.

## 2026-10-20 11:0x – Lỗi Java khi chạy 04_benchmark trên Windows của người dùng
- Triệu chứng: "Error: could not open `C:\Program Files\Android\openjdk\jdk-21.0.8\lib\jvm.cfg'". where.exe java -> C:\Program Files (x86)\Common Files\Oracle\Java\javapath\java.exe (tốt); $env:JAVA_HOME = C:\Program Files\Android\openjdk\jdk-21.0.8 (hỏng). PySpark ưu tiên JAVA_HOME nên dùng bản hỏng. (Nguyên nhân JAVA_HOME bị đổi giữa chừng: suy đoán do Android Studio, chưa xác minh.)
- Cách sửa cho người dùng: PowerShell: Remove-Item Env:JAVA_HOME (chỉ phiên đó); vĩnh viễn: xoá/sửa JAVA_HOME trong Environment Variables.
- Sửa code: jobs/common.py thêm ensure_working_java() (gọi trong get_spark): nếu java của JAVA_HOME không chạy được thì bỏ JAVA_HOME, in cảnh báo, dùng java trong PATH. Đã thử trên Linux với JAVA_HOME giả; chưa thử trên Windows.

## 2026-10-20 11:3x – scripts/collect_results.py
- Gom mọi file kết quả (csv/json/txt/log/md nhỏ trong results/, riêng spark_ui chỉ *_summary.csv + capture_info.json; --png-main/--png-all/--json tuỳ chọn) + NOTES.md vào handoff/ kèm MANIFEST.txt và zip handoff_YYYYmmdd_HHMM.zip. Chỉ sao chép. Đã thử trên kết quả giả; chưa chạy trên máy người dùng.
- handoff/ và handoff_*.zip là file do script này sinh ra (scripts/collect_results.py).

## 2026-10-20 11:4x – KẾT QUẢ THẬT 04_benchmark.py (máy người dùng: Windows 10, 12 nhân logic, Python 3.11.9, PySpark 3.5.3, pandas 3.0.6, pyarrow 26.0.0; Spark local[2], driver 3g; mỗi cấu hình 1 khởi động + 3 lượt đo)
- Tính đúng: mọi lượt khởi động đều in tổng kiểm chứng; Spark và Pandas ra CÙNG số chuyến và doanh thu (vd 6 tháng: 19.938.934 chuyến, 562.837.509 USD); 12 tháng: 40.276.762 chuyến, 1.159.028.317 USD (tổng total_amount; khớp ước tính +2% so với bản 1 ở 02b).
- scale (median): Spark 1/3/6/12 tháng = 0,48/0,99/1,83/3,56 s -> 6,1/9,5/10,9/11,3 triệu dòng/s (thời gian tăng 7,4 lần khi dữ liệu tăng 12 lần: có chi phí cố định ~0,2–0,3 s). Pandas 1/3/6 tháng = 0,36/1,12/2,14 s. Pandas NHANH hơn ở 1 tháng (x1,33), Spark nhanh hơn ở 3 tháng (x1,13) và 6 tháng (x1,17). Chưa chạy Pandas 12 tháng và chưa đo bộ nhớ -> CHƯA chứng minh được "Pandas hết RAM"; lợi thế của Spark trên cỡ dữ liệu này là nhỏ.
- shuffle (12 tháng, 2 nhân, AQE tắt): partitions 2/8/16/64/200 = 5,79/3,63/3,57/4,07/5,51 s; AQE bật với 200 = 4,70 s. Tốt nhất 8–16 (4–8 partition/nhân); 200 chậm hơn 54% so với 16; AQE bật rút ngắn còn +32% (không loại hết). partitions=2 có phương sai lớn (3,48–6,23 s; chưa rõ nguyên nhân, chỉ 3 lượt).
- format (3 tháng): Parquet 0,90 s so với CSV 13,65 s = nhanh hơn 15,2 lần; đĩa Parquet 336,8 MB so với CSV 1.950 MB = CSV lớn 5,79 lần. Spark UI: byte đọc vào 121 MB (Parquet, chỉ cột cần) so với 1.951 MB (CSV, đọc trọn) = 16 lần. Hai yếu tố (nén + chỉ đọc cột cần) bị gộp, chưa tách riêng. Thời gian tạo CSV KHÔNG được ghi vì thư mục CSV đã có từ lần chạy --quick trước (cần --rebuild-csv nếu muốn có số này).
- pruning (tháng 6): lọc theo cột phân vùng 0,16 s so với lọc bằng hàm 2,07 s (12,9 lần); Spark UI: byte đọc 7,6 MB so với 395 MB (51,7 lần ít hơn), số dòng quét 3,46 triệu so với 40,3 triệu.
- cores (12 tháng): local[1/2/4/8] = 6,11/3,47/2,33/2,12 s -> tăng tốc 1,76/2,62/2,88 lần (hiệu suất 88%/66%/36%). Lợi suất giảm mạnh sau 4 nhân; Spark UI: chỉ 17 task cho 12 tháng (mỗi tệp một task) nên song song hoá bị chặn -> đây là giả thuyết hợp lý, chưa kiểm chứng thêm.
- Spark UI (thư mục pruning, cả phiên): không có spill ra đĩa (0 byte); shuffle ghi 3,6–6,5 MB cho 12 tháng (95 nghìn nhóm) – nhỏ; 251 stage (56 bị bỏ qua).
- kpi_daily mới: tip_pct_card ổn định 20,5–23,6% (TB 21,9%) trong khi mean_tip_ratio_card_pct vẫn nhảy (29/1: 89,5%); số chuyến tip > fare ngày 29/1 chỉ 112 (bình thường 83–241) -> không phải do nhiều chuyến tip > fare. Giả thuyết hợp lý nhất (chưa kiểm chứng trực tiếp): vài dòng quãng đường 0 có fare 0,01–1 USD (không bị luật FARE_BELOW_MIN vì luật chỉ áp khi quãng đường > 0) làm tỉ lệ tip/fare lên hàng chục nghìn %. Khi phân tích tip nên dùng tỉ lệ tổng hoặc loại is_zero_distance / fare < 3.
- Hạn chế khi viết báo cáo: 3 lượt đo trên một máy; cache hệ điều hành làm các lượt sau nhanh hơn lượt khởi động; khối lượng công việc chỉ đọc 4 cột nên thông lượng cao; số 12 nhân là nhân logic.
- Việc kế tiếp (đề xuất): (a) jobs/05_make_figures.py vẽ biểu đồ cho báo cáo từ bench_summary + KPI; (b) quyết định có thêm thử nghiệm quy mô với HVFHV (1–2 tháng, ~20 triệu dòng/tháng) và/hoặc Pandas 12 tháng + đo RAM; (c) Docker, sơ đồ luồng dữ liệu, chạy lại từ thư mục sạch, báo cáo, phụ lục AI, bảng đóng góp.


## Bước 5 – 05_make_figures.py
- 11 hình trong results/05_make_figures/ (fig01..fig11) + figures_index.csv. Số liệu lấy từ 04_benchmark, 03_aggregate, 02_clean*, spark_ui/*/stages_summary.csv.
- Kết luận cần thận trọng: Spark chỉ nhanh hơn Pandas nhẹ từ 3 tháng; cores 8 nhân chỉ ×2,88 (giả thuyết: ít task); "Pandas hết RAM" CHƯA chứng minh.
- Còn tồn: nhãn "0" ở fig11 hơi lạ; nhãn cuối fig01 panel phải sát mép.

## Bước 4b – 04b_pandas_memory.py (đã chạy thật 2026-10-21)
- Mục tiêu: kiểm chứng "Pandas hết RAM ở 12 tháng?" bằng RSS đỉnh; 3 động cơ: pandas_narrow (4 cột), pandas_wide (mọi cột), spark.
- Mỗi lượt = tiến trình con; cha đo RSS cả cây; trạng thái OK/MEMORY_ERROR/KILLED/OVER_LIMIT/TIMEOUT; --limit-gb giả lập RAM nhỏ.
- Thêm psutil vào requirements. HVFHV vẫn chưa làm (cần tải ~ 20 triệu dòng/tháng; chờ quyết định của user).

### Kết quả 04b (máy thật: Windows 10, 12 CPU, RAM TỔNG 15,8 GB — không phải 8 GB; trống 4,9 GB lúc bắt đầu)
- 12 tháng (40.276.762 dòng), median 3 lượt: Spark 1,56 GB / 6,3 s; Pandas 4 cột 4,19 GB / 3,2 s; Pandas mọi cột 10,2 GB / 20,3 s (chậm hẳn: nghi bộ nhớ bị nén/phân trang vì RAM trống chỉ 4,9 GB).
- Kết luận CẨN THẬN: Pandas KHÔNG hết RAM trên máy này (cả bản đọc mọi cột vẫn chạy). Chỉ nói được: RAM Pandas tăng tuyến tính theo dữ liệu (~0,35 GB/tháng với 4 cột; ~0,85 GB/tháng mọi cột), còn Spark gần phẳng (bị chặn bởi driver-mem 3g). Trên máy 8 GB thì bản đọc mọi cột (10 GB) sẽ không chạy — đây là SUY LUẬN, chưa đo (muốn chứng minh: --limit-gb 8).
- Pandas 4 cột nhanh hơn Spark ở 04b vì Spark khởi động JVM mới mỗi lượt, kế hoạch có thêm agg tổng; thời gian Spark 04b KHÔNG so được với 04_benchmark (đã khởi động nóng). Doanh thu/số chuyến kiểm chứng khớp giữa 3 động cơ.
- Đổi fig01: tiêu đề trung tính "cùng cỡ"; thêm fig12.

### 04b với --limit-gb 8 (pandas_wide, 12 tháng) — 2026-10-21
- Kết quả: OVER_LIMIT, tiến trình bị giết sau 2,8 s khi RSS chạm 8,05 GB (RAM trống lúc đó 9,4 GB). Bản đọc mọi cột cần ~10 GB (đo ở lần trước) nên KHÔNG chạy trong ngân sách 8 GB.
- Giới hạn của phép thử: đây là giả lập "tiến trình không quá 8 GB", không phải máy 8 GB thật (HĐH + ứng dụng khác còn ăn thêm), nên thực tế còn khó hơn. Pandas 4 cột (4,2 GB) vẫn nằm trong ngân sách.
- Lần chạy có --limit-gb nay ghi vào results/04b_pandas_memory/limit_<N>gb/ (trước đó script ghi đè cùng thư mục).

## Bước 6 – HVFHV 1–2 tháng (code xong, đã thử trên dữ liệu GIẢ; chưa có kết quả thật)
- Môi trường cloud không tải được CloudFront (403) nên user tự tải: download_data.py --type fhvhv --start 2024-01 --end 2024-02.
- Ingest dùng 01 --type fhvhv (giữ cột, chữ thường). Lake giữ TIMESTAMP_NTZ -> khi trừ mốc thời gian phải cast("timestamp") (đã xử lý trong 06).
- 06_profile_hvfhv.py: hv_profile_by_month, hv_anomalies_by_month (miles<=0, time<=0, fare<=0, driver_pay<0, ngoài tháng, chờ>1h, >80 mph, lệch trip_time vs timestamp…), hv_license_mix, hv_nulls, hv_quantiles. Tên hãng theo data dictionary TLC (HV0002 Juno, 0003 Uber, 0004 Via, 0005 Lyft).
- 04b --dataset fhvhv: cùng phép gom (ngày × khu đón), tổng = base_passenger_fare, trung bình = driver_pay.
- tests/make_fake_fhvhv.py: sinh dữ liệu giả cùng schema (chỉ để thử code).
- Lưu ý: toPandas() của pyspark 3.5 lỗi trên Python 3.13 (thiếu distutils) -> 06 ghi CSV bằng module csv.

### Kết quả HVFHV thật (2024-01, 2024-02) — 2026-10-21
- 19.663.930 + 19.359.148 = 39.023.078 dòng (≈ Yellow cả 12 tháng 40,3 triệu). Chỉ có HV0003 Uber 73,91% và HV0005 Lyft 26,09% (không có Juno/Via ở 2 tháng này). Ngày đón nằm trọn trong tháng; không có dropoff<pickup; trip_time khớp mốc giờ (lệch>60s = 0); tips không âm.
- Null: originating_base_num và on_scene_datetime đều null 26,02% (10.155.624 dòng) — gần bằng số chuyến Lyft (10.181.018) nhưng không bằng; GIẢ THUYẾT: một hãng không điền hai cột này, CHƯA kiểm (cần crosstab theo hvfhs_license_num).
- Bất thường (T1+T2): đón trước giờ gọi xe 387.471 (0,99%; wait_s p1 = −26 s, min −2.863 s); driver_pay > 1,5×base_fare 78.465 (T1, 0,40%) → 318.417 (T2, 1,64%): nhảy ×4 giữa hai tháng, CHƯA giải thích (driver_pay gồm cả tip/toll nên tỉ lệ này không hẳn là lỗi); fare ≤ 0: 15.922 (0,04%); miles ≤ 0: 6.368; miles > 100: 2.944; driver_pay < 0: 243; chuyến > 6 giờ: 132; >80 mph: 56; time ≤ 0: 5. Cực trị: max base_fare 1.911 USD, max trip_miles 417,6, max trip_time 55.138 s.
- Số liệu chính: avg_base_fare 23,96/24,27 USD; avg trip 18,5/19,2 phút; avg_driver_pay 18,27/19,07; tổng base fare 471,1 / 469,8 triệu USD (T2 ít ngày hơn nhưng doanh thu gần bằng).
- 04b HVFHV (median 3 lượt): 1 tháng: Spark 1,74 GB/3,4 s; Pandas 4 cột 2,24 GB/1,5 s; mọi cột 8,92 GB/3,7 s. 2 tháng: Spark 1,98 GB/4,4 s; Pandas 4 cột 4,26 GB/2,8 s; mọi cột 10,4 GB/19,9 s.
- Nhận xét: RAM Pandas 4 cột ở 39 triệu dòng HVFHV (4,26) ≈ ở 40,3 triệu dòng Yellow (4,19) → RAM theo số dòng × cột đọc. Pandas mọi cột 2 tháng chậm ×5,4 so với 1 tháng dù dữ liệu ×2 và RSS chỉ tăng 8,9→10,4 GB: DẤU HIỆU RAM trống ~9 GB bị chạm trần (nén/phân trang), nên RSS đỉnh có thể thấp hơn nhu cầu thật (giả thuyết, chưa chứng minh).
- Thêm fig13 (RAM theo số dòng, hai bộ) và fig14 (bất thường HVFHV).

## 06b_diagnose_hvfhv.py (code xong, thử trên dữ liệu giả; chờ kết quả thật)
- H1 null 26,02% theo hãng; H2 đón trước giờ gọi theo hãng×tháng (+ độ lệch giây, có kèm on_scene null?); H3 driver_pay>1,5×fare theo hãng×tháng, theo NGÀY, theo nhóm giá, theo tip/toll/phí sân bay, và sau khi trừ tip+toll+airport_fee.
- 7 file CSV trong results/06b_diagnose_hvfhv/. Cần user chạy và gửi lại.

### Kết quả 06b thật (2026-10-21) — giải thích ba bất thường HVFHV
- H1 null: on_scene_datetime và originating_base_num null ở LYFT 99,76% (T1) / 99,74% (T2) (5.218.737 / 4.936.887 dòng), UBER 0%. Hai cột trùng số null → Lyft hầu như không điền cả hai (≈0,25% Lyft có giá trị). Giả thuyết cũ ĐÃ XÁC NHẬN. Hệ quả: không dùng on_scene_datetime/originating_base_num cho cả HVFHV; chỉ có ý nghĩa với Uber.
- H2 đón trước giờ gọi: Lyft 1,79% (T1) / 2,06% (T2); Uber 0,66% / 0,68% → ổn định hai tháng, không liên quan cú nhảy T2. Trung vị độ lệch Lyft đúng −900 s (−15 phút), Uber −687/−742 s; min −2.863 s. Số −900 tròn gợi ý hiện vật của cách ghi request_datetime (GIẢ THUYẾT, chưa kiểm). 93% dòng âm của Lyft có on_scene null (gần như bằng tỉ lệ null chung, nên không đặc biệt).
- H3 driver_pay > 1,5×fare (fare>0): Lyft 0,41% → 0,36% (phẳng). UBER 0,35% → 2,03%. Theo NGÀY: Uber chỉ nhảy trong 7 ngày 5/2–11/2 (7,0–8,2%/ngày, TB 7,5%), ngày 12/2 còn 0,93% rồi về ~0,25%. 260.949 / 292.170 = 89% số dòng vượt của Uber tháng 2 nằm trong tuần đó; các ngày còn lại của T2 chỉ 0,285% (thấp hơn T1 0,346%). Lyft không đổi trong tuần đó. Cú nhảy theo tháng KHÔNG phải thay đổi cấu trúc mà là một tuần bất thường.
- Tuần đó: ratio_mean ngày 0,91–0,96 so với ~0,76 bình thường; dồn vào chuyến rẻ (nhóm 5–10 USD: 0,64% → 5,39%; 10–20: 0,36% → 2,33%; 20–50: 0,24% → 0,63%; ≥50 không đổi 0,048%). Tip/toll/phí sân bay giải thích rất ít: sau khi trừ chúng, Uber T2 vẫn 1,83% (so 2,03%).
- NGUYÊN NHÂN thật (khuyến mãi/trợ cấp tài xế? lỗi ghi driver_pay? thay đổi mức trả tối thiểu?) KHÔNG xác định được từ dữ liệu; ghi là "chưa rõ", đừng khẳng định.
- Ngoài ra 1/1/2024 cao bất thường ở cả hai hãng (Lyft 1,78%, Uber 1,16%) — đêm giao thừa/ngày lễ, chưa kiểm.
- Lưu ý nhỏ: số 78.465/318.417 của 06 tính cả dòng fare ≤ 0 nên lớn hơn 71.123/309.785 (chỉ fare>0) ở 06b.
- Ý nghĩa cho làm sạch HVFHV (nếu làm): đừng loại theo ngưỡng cố định driver_pay/fare; gắn cờ theo (hãng, ngày). Thêm fig15.


## Docker (2026-10-21)
- Thêm Dockerfile (python:3.11-slim-bookworm + openjdk-17-jre-headless), docker-compose.yml (volume data/results/demo_out, mem_limit APP_MEM mặc định 6g), .dockerignore, scripts/docker_demo.sh.
- docker_demo.sh chạy trong BẢN SAO tạm (mktemp) nên không ghi đè data/results thật; kết quả chép ra demo_out/. Các bước: 00 check_env, fake data 3 tháng×200k, 01 ingest, 02_clean_v2, 03 aggregate, 04 --quick scale format, 04b 1–2 tháng, 05 (6 hình: fig01 03 06 07 10 12).
- ĐÃ TEST: chạy bash scripts/docker_demo.sh trực tiếp (Python 3.13 + Java của môi trường cloud) → exit 0, 6 hình.
- ĐÃ TEST BỞI USER (2026-10-21): docker compose build + docker compose run --rm app chạy thành công trên Windows/WSL2 (container Linux, Python 3.11.17, OpenJDK 17.0.20.1, PySpark 3.5.3; RAM WSL2 mặc định 7,7 GB, trống 6,9 GB); 7 bước OK, 6 hình. Bằng chứng: results/docker_demo/demo_log.txt (sinh bởi scripts/docker_demo.sh).
- Chưa có Docker thì vẫn chạy bình thường bằng .venv như cũ.

## Sơ đồ (2026-10-21)
- scripts/make_diagrams.py (matplotlib, không cần Graphviz) -> docs/diagrams/diagram01_data_flow và diagram02_clean_policy (.png 200 dpi + .svg).
- Số liệu trong sơ đồ đọc từ results/02_clean_v2 (41.169.720 = 4 trùng + 892.954 loại + 40.276.762 đạt) và 06_profile_hvfhv (39.023.078 dòng HVFHV); thiếu results thì bỏ số.
- Lưu ý ô LOẠI: một dòng vi phạm nhiều luật nên số các luật cộng lại > số dòng bị loại (đã ghi ở chân sơ đồ).
- Chưa vẽ: sơ đồ triển khai/kiến trúc cụm (dự án chạy Spark local[N], không có cụm) — nếu giảng viên đòi, nói rõ giới hạn này ở mục 8.

## Report (Claude Docs) – progress 2026-10-21
- Done in Doc: cover (blanks), section 1, section 2 (2.1 RDD/Spark SQL, 2.2 Parquet, 2.3 Spark tuning params, 2.4 TLC dictionaries, 2.5 gaps).
- Sources opened & read (for References): [1] Zaharia et al., RDD, NSDI'12, pp.15-28 (usenix page + paper PDF); [2] Armbrust et al., Spark SQL, SIGMOD 2015 (amplab page + doi); [3] Parquet docs "Concepts"; [4] Parquet "File Format"; [5] Spark SQL Performance Tuning (3.3.1 page; 3.5.3 page blocked by robots -> flagged); [6] TLC Trip Record Data page; [7] Yellow data dictionary (18 Mar 2025); [8] HVFHS data dictionary (18 Mar 2025).
- Not stated by sources (labelled as report inference): columnar => fewer bytes vs CSV; best shuffle partitions on single machine.
- Report section 3 written (3.1 sources, 3.2 Yellow schema, 3.3 HVFHV schema, 3.4 measured data-quality table, 3.5 input/output, 3.6 requirements F1-F5 + non-functional, 3.7 sample record = PLACEHOLDER awaiting user's output of scripts/sample_records.py). Fixed: Helix rows = 230 (was mis-typed 7.230 in sec 2); garbled cell in 3.4 table fixed.
- Section 3 cross-refs to later sections: 4.3 (clean policy), 5 (ingest/Docker 5.4), 6.4 (HVFHV vs Yellow RAM), 8 (HVFHV findings). Keep numbering consistent when writing 4-8.

## 2026-10-21 – Phat hien tu sample_records.txt (CHUA kiem chung)
- Dong clean Oct co tpep_pickup = 2024-09-30 19:27:39 (doc bang pyarrow) nhung pickup_hour=2, pickup_dow=3 (Thu Ba) -> khop gio 02:27 ngay 2024-10-01 (UTC+7). Nghi: JVM/Spark tren may nguoi dung dung mui gio Asia/Bangkok; timestamp tho (khong mui gio) bi doc nhu gio dia phuong roi luu thanh instant UTC => gia tri vat ly trong clean_v2 lech -7h so voi tho khi doc ngoai Spark. Trong Spark, hour/dow/loc thang nhat quan (cung mui gio) nen KPI khong sai; Docker (UTC) khong lech. CHUA xac minh (can doi chieu cung chuyen tho vs sach: chay lai scripts/sample_records.py ban moi).
- Khong co cau hinh spark.sql.session.timeZone trong code (grep rong). De xuat (chua lam): dat UTC trong common.get_spark de ket qua khong phu thuoc may, nhung phai chay lai 01/02/03 -> cho nguoi dung quyet.
- Loi trong ban sample_records cu: glob *2024-01* khop file fhvhv -> da sua, doc dung yellow_tripdata_<ym>.
- 2026-10-22: sample_records.txt (ban sua) XAC NHAN tren 1 ban ghi: tho 2024-10-01 00:30:44 -> sach 2024-09-30 17:30:44 (-7h), pickup_hour=0 khop tho, duration 1062 s, speed 10.17 mph, total 24.9 khop. Nguyen nhan mui gio van la GIA THUYET. Muc 3.7 cua bao cao da dien (bang tho vs sach + HVFHV Uber mau + ghi chu mui gio, hen nhac lai o muc 8). Zip bi loi chong 2 ban do `zip -r` ghi them vao zip cu -> luon rm zip truoc, zip tu thu muc cha.
- 2026-10-22 Report section 4 written (4.1 architecture + diagram01 as Fig1; 4.2 decisions vs evidence table; 4.3 clean policy + diagram02 (Fig2) + v1/v2 table + fig11 (Fig3); 4.4 KPI; 4.5 experiment codes E1-E7 -> Q1-Q4 (E1 format, E2 pruning, E3 shuffle/AQE, E4 cores, E5 scale, E6 RAM Pandas vs Spark, E7 HVFHV); 4.6 limits). USE SAME E-codes in section 6. Images embedded via Artifact asset upload (url = doc link, asset:true) -> Docs create(blob) -> ![alt](blob/id). Uploaded so far: diagram01, diagram02, fig11 (blobs 25434952-8320, 8fb53439-d095, 98a01f80-80e5). Figure numbering in report: Fig1=diagram01, Fig2=diagram02, Fig3=fig11; continue 4+ in section 6/7/8.
- Derived (not directly stated in a file): v1 SPEED_GT_MAX 12,171 = 13,245 - 1,074 (from notes).
- 2026-10-22 Report section 5 written (5.1 env table Windows vs Docker; 5.2 job table with measured times: ingest ~57 s, clean_v2 ~333 s (v1 494 s), aggregate ~19 s; others 'chua ghi'; total clean re-run = [chua do] placeholder; 5.3 Spark defaults local[2]/3g/16 partitions; 5.4 Docker (demo on FAKE data, user ran OK on WSL2; no real-data Docker run); 5.5 errors table (winutils, JAVA_HOME, toPandas py3.13, TIMESTAMP_NTZ, %3D URI, --limit-gb overwrite); 5.6 not done).
- User to-do for section 5: run full pipeline from a clean folder and send timings (placeholder [chua do] in 5.2); optional timings for download/05/06/06b.
- 2026-10-22 Report section 6 written: 6.0 method; 6.1 E1 format; 6.2 E2 pruning (flag: bytes ratio 51.7x vs rows ratio 11.6x -> cause NOT investigated; maybe ask user to check in Spark UI / run extra); 6.3 E3 shuffle + E4 cores; 6.4 E5 scale + E6 RAM (+8GB limit) + E7 HVFHV RAM; 6.5 Q1-Q4 table + threats. Figures: Fig4 format(blob 001621a7-c228), Fig5 pruning(76d69b00-4791), Fig6 shuffle(6c43ab9b-d31b), Fig7 cores(e5041820-87fe), Fig8 scale(4e18e404-e0cb), Fig9 fig12(187619f7-0d8f), Fig10 fig13(8701c770-ad6f).
- Not verified: E5 table 'Pandas 12 months: chua chay' is from 04_benchmark only (04b did run Pandas 12m: 3.2 s narrow). Mentioned separately in E6; ok but could confuse -> consider note. 04 'shuffle partitions=2 variance 3.48-6.23' from notes.
- 2026-10-22 fig10 loi bo cuc: phu de 2 dong de len tieu de panel -> sua jobs/05_make_figures.py (height 4.9, top 0.74). Chua ve lai duoc o cloud (thieu results/03_aggregate); user chay: python jobs/05_make_figures.py --only fig10 va gui fig10_dq_vendor.png.
- 2026-10-22 Report section 7 written (7.1 KPI: Figs 11-14 = fig06-09, blobs 12912991-6af6, 9aaf0b68-6e6a, b64e0faa-187e, cc69a985-c3e9; vendor DQ text + PLACEHOLDER 'Hinh 15' for fig10 (needs re-render, see fig10 note); open items list (JFK->JFK 83,871 NOT handled in kpi_od_top - only suggestion; Flex tip; Helix; tip ratio spike); 7.2 Spark UI table + PLACEHOLDER for PNG screenshots (user must check results/04_benchmark/spark_ui/*.png exist); 7.3 Docker demo table.)
- Remaining placeholders in report: cover blanks, 3.? none, 5.2 total re-run time [chua do], 7.1 Hinh 15, 7.2 Spark UI screenshots, abstract, sections 8-9 + refs + appendices.
- 2026-10-22 fig10 re-rendered by user (layout fixed), copied into results/05_make_figures/fig10_dq_vendor.png; embedded as Fig 15 in report 7.1.

## 2026-10-22 – jobs/04c_capture_spark_ui.py (chup anh Spark UI, yeu cau cua user)
- Ly do: 04_benchmark chi luu anh khi chay --capture-ui + Playwright; Spark UI chet khi Spark dung. Script chay lai 1 luot truy van dai dien (cung NEED_COLS/phep gom/noop nhu 04_benchmark) moi thi nghiem trong SparkSession rieng roi goi ui_capture.capture().
- Thi nghiem: pruning (partition filter vs function filter month=6), shuffle (p16 AQE off, p200 AQE off, p200 AQE on), format (parquet vs csv 3 thang; can data/bench/yellow_csv_3m), scale (--only scale), cores (--only cores). Mac dinh: pruning shuffle format.
- Ra: results/04_benchmark/spark_ui_png/<ten>/ (rieng, khong de spark_ui/ do --capture-ui). setJobDescription de Jobs/Stages co nhan E2/E3...
- DA TEST o cloud tren du lieu GIA (3 thang x 100k dong, Chromium preinstalled qua CHROMIUM_PATH): 68 anh, trang chi tiet SQL hien 'number of files read', 'size of files read', 'number of partitions read' (dung cho bang chung E2). CHUA chay tren Windows cua user; so lieu that se khac.
- Cap nhat README (muc 'Chup anh Spark UI').
