# Vì sao làm sạch dữ liệu như vậy (jobs/02_clean.py)

Mọi con số lấy từ `results/02a_profile_raw/` (41.169.720 dòng, Yellow Taxi 2024). Mục nào là **suy luận chưa kiểm chứng** thì ghi rõ.

## 1. Nguyên tắc chung (5 ý có thể dùng để bảo vệ)

**N1. Luật theo miền nghiệp vụ, không cắt đuôi thống kê.**
Cắt ở p99 sẽ xoá chuyến thật: p99 quãng đường = 20 dặm, p99 giá = 80 USD, và chuyến ra sân bay JFK dài cỡ 17 dặm, giá cố định 70 USD. Phần đuôi đó là dữ liệu đúng. Mình chỉ loại giá trị *không thể xảy ra* (thời lượng âm, tốc độ 100 mph trong thành phố, 398.609 dặm dài hơn chu vi Trái Đất 24.901 dặm) hoặc *không thuộc loại "chuyến đi"* (dòng hoàn tiền).

**N2. Ba nhóm hành động, không chỉ "xoá":**
- **LOẠI** khi cả dòng không tin được hoặc không phải chuyến đi.
- **SỬA** khi chỉ một ô sai, phần còn lại tốt (passenger_count).
- **GIỮ** khi dữ liệu thiếu có cấu trúc nhưng chuyến là thật (Flex Fare).

**N3. Không xoá hẳn: dòng bị loại vào `yellow_rejected` kèm `reject_reasons`.**
Lý do: (a) kiểm toán được, báo cáo có bảng "trước/sau"; (b) đảo ngược được, đổi ngưỡng rồi chạy lại; (c) chính tập bị loại là dữ liệu để phân tích chất lượng (lỗi tập trung ở tháng nào, mã nào).

**N4. Không bù giá trị (imputation).** Điền passenger_count = 1 hay trung vị là bịa dữ liệu và làm lệch thống kê. Để null và gắn cờ trung thực hơn.

**N5. Có thể kiểm chứng và tái lập.** Mỗi tháng phải thoả `rows_in = trùng + loại + đạt` (không mất dòng thầm lặng). Ngưỡng là tham số dòng lệnh và được ghi vào `results/02_clean/clean_params.json`.

## 2. Từng luật: lý do, số liệu, điểm yếu

| Luật | Số dòng (profile) | Quyết định |
|---|---|---|
| TIME_NULL | 0 | Loại (chốt chặn) |
| DUR_NONPOSITIVE | 1.575 (âm) + 11.935 (bằng 0) = 13.510 (0,033%) | Loại |
| DUR_GT_MAX (> 24 giờ) | 230 | Loại |
| Thời lượng 3–24 giờ | 25.119 | **Giữ** |
| Thời lượng 1–59 giây | 488.060 (1,19%) | **Giữ** |
| PICKUP_OUT_OF_MONTH | 420 (ngoài năm: 56) | Loại |
| DIST_NONPOSITIVE | 776.305 (1,89%) | Loại (**quyết định có tranh cãi nhất**) |
| DIST_GT_MAX (> 100 dặm) | 1.613 | Loại |
| SPEED_GT_MAX (> 100 mph) | 12.171 | Loại |
| FARE_NONPOSITIVE | 731.024 (âm) + 17.260 (bằng 0) = 748.284 (1,82%) | Loại |
| FARE_GT_MAX (> 500) / TOTAL_GT_MAX (> 1000) | 600 / 43 | Loại |
| TOTAL_NONPOSITIVE | 609.344 (âm) + 5.062 (bằng 0) | Loại |
| NEG_COMPONENT | tolls 49.272, extra 304.569, mta_tax 583.164, tip 1.331 | Loại |
| ZONE_INVALID | 0 | Loại (chốt chặn) |
| Mã khu vực 264/265 (Unknown) | PU 146.201; DO 370.258 | **Giữ** |
| passenger_count = 0 hoặc > 6 | 401.354 + 284 | **Sửa** thành null |
| passenger_count null (và 4 cột đi kèm) | 4.091.232 (9,94%) | **Giữ** |
| vendorid 6, 7 | 2.069; 230 | **Giữ** |
| Dòng trùng hệt | 5 (khoá 8 cột) | Loại |

### 2.1 Thời gian
- **Thời lượng ≤ 0:** giờ trả trước giờ đón là bất khả; bằng 0 giây thì chuyến không diễn ra hoặc đồng hồ lỗi, và tốc độ không tính được (chia cho 0). Không thể biết cột nào sai (giờ đón hay giờ trả) nên không sửa được, chỉ loại.
- **> 24 giờ:** p99 thời lượng là 4.229 giây (70 phút); 24 giờ gấp ~20 lần. Ngưỡng này *cố ý rộng*.
- **Vì sao giữ 3–24 giờ (25.119 dòng):** có thể là chuyến liên tỉnh, chờ khách lâu hoặc quên tắt đồng hồ, mình không có bằng chứng để phân biệt. Các luật tốc độ và giá đã chặn phần vô lý trong nhóm này. Đây là điểm nên chạy kiểm tra độ nhạy (mục 4).
- **Vì sao giữ 1–59 giây (488.060 dòng):** có thể là khách huỷ ngay sau khi lên xe. Chưa có căn cứ coi là lỗi; ảnh hưởng tới KPI nhỏ vì doanh thu của các chuyến này cũng nhỏ. Nên kiểm tra thêm bằng bảng chéo với `fare_amount`.
- **Ngày đón ngoài tháng của file (420 dòng):** bước nạp phân vùng theo *tên file*, nên nếu giờ đón lệch tháng thì mốc thời gian không đáng tin (ví dụ năm 2002 do đồng hồ thiết bị đặt lại). Các KPI theo giờ/ngày sẽ sai nếu giữ.

### 2.2 Quãng đường và tốc độ
- **Quãng đường > 100 dặm (1.613 dòng):** giá trị lớn nhất 398.608 dặm chứng minh có lỗi nhập. Từ 100 dặm trở lên hiếm khi xác minh được; 100 dặm vượt xa vùng 5 quận. Ngưỡng bảo thủ (p99 chỉ 20 dặm).
- **Tốc độ trung bình > 100 mph (12.171 dòng):** tốc độ trung vị thực tế ≈ 1,76 dặm / 780 giây ≈ 8 mph. 100 mph là bất khả trong thành phố, báo hiệu quãng đường hoặc thời gian sai. Tương tự, không biết cột nào sai nên loại.
- **Quãng đường bằng 0 (776.305 dòng, 1,89%) – cần nói thẳng điểm yếu:** p1 của trip_distance là 0, khớp với 1,89%. Lý do loại: không tính được tốc độ, doanh thu theo dặm và luồng khu vực → khu vực; còn các dòng này nhiều khả năng là khách huỷ hoặc đồng hồ không ghi quãng đường. Nhưng chúng *có thể* vẫn có tiền thật (khách đi vòng về chỗ cũ, pu = do). Loại chúng làm doanh thu tổng thấp hơn đôi chút. Giảm thiểu: dòng vẫn nằm trong `yellow_rejected`, có thể đưa vào lại.

### 2.3 Tiền
- **Giá âm (731.024) và các khoản phụ âm (mta_tax 583.164; extra 304.569):** các cột âm *cùng lúc* trên cùng loại dòng cho thấy đó là dòng đảo giao dịch (hoàn tiền/huỷ/tranh chấp), không phải chuyến. `payment_type` = 4 (tranh chấp) có 794.494 dòng và = 3 (miễn phí) có 291.743 dòng, cùng bậc độ lớn. **Đây là suy luận, chưa có bảng chéo `payment_type × tiền âm` để khẳng định.** Lý do loại khỏi tập chuyến đi: nếu giữ, giá trung bình và tổng doanh thu bị kéo lệch.
- **Điểm yếu cần nhớ:** vì không có mã chuyến để ghép dòng hoàn tiền với chuyến gốc, loại cả hai nhóm không đảm bảo triệt tiêu: chuyến gốc (dương) vẫn còn trong dữ liệu đạt. Doanh thu có thể hơi bị ước cao.
- **Giá = 0 (17.260), tổng = 0 (5.062):** không phát sinh doanh thu, loại khỏi KPI doanh thu.
- **Giá > 500 (600 dòng), tổng > 1000 (43 dòng):** p99 giá chỉ 80; giá tối đa 335.544 USD là bất khả. Ngưỡng 500 hơi tuỳ ý (chuyến liên bang có thể 400–800 USD), nhưng số dòng ảnh hưởng chỉ ~0,0015%.
- **Bất kỳ khoản phụ âm** (kể cả `tolls_amount` 49.272 dòng): một khoản âm báo hiệu điều chỉnh. Không thể tin phần còn lại của dòng nên loại cả dòng. Nhiều khả năng phần lớn trùng với dòng giá âm (chưa đo độ chồng lấn).
- **`total_amount < fare_amount` (603.870 dòng) không có luật riêng:** gần như trùng nhóm `total_amount < 0` (609.344), nên các luật trên đã bao phủ. Nên kiểm tra lại sau làm sạch rằng số dòng còn lại gần 0.

### 2.4 Khu vực
- **Mã không thuộc bảng khu vực:** 0 dòng, luật giữ lại như chốt chặn cho dữ liệu năm sau.
- **264/265 (Unknown) giữ nguyên:** đây là mã hợp lệ của TLC nghĩa là "không rõ vị trí". Không phải lỗi dữ liệu mà là thiếu thông tin (0,36% điểm đón, 0,90% điểm trả). Việc có loại khỏi phân tích khu vực hay không phụ thuộc câu hỏi nên quyết định ở bước KPI, không ở bước làm sạch.

### 2.5 Hành khách, thanh toán, nhà cung cấp
- **passenger_count = 0 (401.354) hoặc > 6 (284; giá trị 7, 8, 9): SỬA thành null.** Số 0 thường là mặc định tài xế quên nhập; xe taxi không chở hơn 6 người. Toàn bộ phần còn lại (giá, quãng đường) vẫn tốt, loại cả dòng gây mất dữ liệu doanh thu không cần thiết. Cột `passenger_count_repaired` giữ dấu vết.
- **9,94% dòng (4.091.232) null đồng thời ở 5 cột và `payment_type` = 0:** các con số trùng nhau từng tháng, nên là *cùng một tập dòng*, tức thiếu có cấu trúc chứ không ngẫu nhiên. Chúng có giá và quãng đường hợp lệ; xoá sẽ mất ~10% chuyến thật và lệch về phía loại thanh toán khác. Giữ lại, thêm cờ `is_flex_fare`. Cần đối chiếu data dictionary của TLC để xác nhận `payment_type` = 0 là "Flex Fare" trước khi khẳng định trong báo cáo.
- **vendorid 6 và 7 (2.069 và 230 dòng):** mã nhà cung cấp mới nhưng hợp lệ; số dòng nhỏ, không có dấu hiệu lỗi. Giữ.

### 2.6 Dòng trùng
Chỉ 5 dòng trong 41 triệu. Loại theo *toàn bộ cột nghiệp vụ* (không dùng khoá 8 cột như lúc profiling) để chỉ xoá bản sao chắc chắn, không xoá nhầm hai chuyến khác nhau nhưng cùng vài trường.

## 3. Cách code hiện thực các quyết định trên

| Ý | Đoạn trong `02_clean.py` | Vì sao |
|---|---|---|
| Luật nhận giá trị NULL | `rules = [(c, d, F.coalesce(cond, F.lit(False))) ...]` | Trong Spark, `when(NULL)` coi như sai và dòng lọt qua âm thầm. Bọc `coalesce` để mọi hành vi với null là có chủ đích. |
| Ghi nhiều lý do | `F.concat_ws("|", *[F.when(c, F.lit(code)) ...])` | `concat_ws` bỏ qua null, nên dòng sạch cho chuỗi rỗng, dòng sai ra "DUR_GT_MAX\|PICKUP_OUT_OF_MONTH". Biết độ chồng lấn giữa các luật. |
| Khoản phụ âm | `coalesce(col, 0.0) < 0` | `cbd_congestion_fee` null 100% ở 2024; null không được tính là âm. |
| Loại trùng | `dropDuplicates(BUSINESS_COLS)` | Chỉ 20 cột nghiệp vụ, không gồm `source_file/year/month`. |
| Xử lý từng tháng | vòng `for y, m in months` + `persist(MEMORY_AND_DISK)` | Máy 8 GB: một tháng ~3,5 triệu dòng vừa bộ nhớ; chuyển sang đĩa khi thiếu. |
| Chạy lại an toàn | `partitionOverwriteMode=dynamic` + `mode("overwrite")` | Chạy lại một tháng chỉ thay đúng phân vùng. |
| Không mất dòng | `ok = rows_in == dups + rejected + clean and back == n` | Kiểm tra bảo toàn trước và sau khi ghi. |
| Có thể đổi ngưỡng | `--max-distance`, `--max-speed`, ... → `clean_params.json` | Làm phân tích độ nhạy; tái lập đúng cấu hình. |

## 4. Điểm yếu nên kiểm tra trước khi nộp (thành phần của mục "Thảo luận")
1. **Bảng chéo `payment_type × (fare < 0, total < 0)`** trên `yellow_rejected` để xác nhận hay bác bỏ giả thuyết hoàn tiền.
2. **Độ nhạy của ngưỡng:** chạy lại với `--max-duration-h 6`, `--max-distance 60`, `--max-speed 80`; so tổng chuyến, doanh thu, giá trung bình. Nếu KPI đổi dưới ~1% thì kết luận không phụ thuộc ngưỡng, rất thuyết phục.
3. **Quãng đường bằng 0:** thử giữ lại nhóm có `pulocationid ≠ dolocationid` và thời lượng > 60 giây để xem chúng có doanh thu thật không.
4. **Kiểm tra sau làm sạch:** `total_amount < fare_amount`, `fare ≤ 0`, thời lượng ≤ 0 phải gần như bằng 0.
5. **Tỉ lệ loại tổng cộng:** chưa biết cho tới khi chạy. Cận dưới là luật lớn nhất (1,89%), cận trên là tổng các luật (~7,5%) vì các luật chồng lấn mạnh.

## 5. Câu trả lời mẫu khi bị hỏi "vì sao loại/giữ X?"
"Nhóm xác định luật theo giới hạn vật lý và nghiệp vụ, không cắt theo phân vị, vì phần đuôi (chuyến sân bay) là dữ liệu thật. Dòng bị loại không bị xoá mà lưu kèm lý do, và ngưỡng là tham số nên có thể kiểm tra độ nhạy. Với những chỗ chưa chắc (hoàn tiền, Flex Fare), nhóm giữ nguyên hoặc gắn cờ thay vì bịa giá trị."
