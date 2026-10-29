"""Chụp TỰ ĐỘNG Spark UI (Jobs, Stages, Storage, Environment, Executors, SQL + trang chi tiết từng truy vấn) khi Spark còn chạy.

Hai nguồn, bổ sung cho nhau:
  1) REST API của Spark (luôn dùng được, không cần cài thêm): lưu JSON gốc + CSV tóm tắt (jobs, stages, sql, executors).
     -> đủ số liệu để vẽ biểu đồ/bảng, không bỏ sót dòng nào như chụp tay.
  2) Ảnh chụp trang UI đầy đủ (full page) bằng Playwright, nếu đã cài:
        pip install playwright
        python -m playwright install chromium
     Không cài thì chỉ thiếu ảnh PNG; phần REST vẫn chạy.

Dùng:  from ui_capture import capture ; capture(spark, Path("results/.../spark_ui/scale"), tag="scale")
UI chỉ tồn tại khi SparkSession còn sống, nên phải gọi TRƯỚC spark.stop().
"""
import csv
import json
import os
import urllib.request
from pathlib import Path


_SEEN = {}  # app_id -> {"sql": set, "stage": set}: chỉ chụp trang chi tiết của truy vấn/stage MỚI kể từ lần capture trước


def _get(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def _write_csv(path, rows, cols):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") for c in cols])


def capture(spark, outdir, tag="", screenshots=True, max_sql=25, top_stages=5):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    ui = spark.sparkContext.uiWebUrl
    if not ui:
        print("[ui_capture] Spark UI không bật (spark.ui.enabled=false)")
        return {}
    app_id = spark.sparkContext.applicationId
    api = f"{ui}/api/v1/applications/{app_id}"
    summary = {"ui": ui, "app_id": app_id, "tag": tag}

    jobs = _get(f"{api}/jobs")
    stages = _get(f"{api}/stages")
    try:
        sql = _get(f"{api}/sql?length=1000")
    except Exception:  # noqa: BLE001 – API /sql chỉ có từ Spark 3.0
        sql = []
    executors = _get(f"{api}/allexecutors")
    for name, data in (("jobs", jobs), ("stages", stages), ("sql", sql), ("executors", executors),
                       ("environment", _get(f"{api}/environment"))):
        (outdir / f"{name}.json").write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")

    _write_csv(outdir / "jobs_summary.csv", jobs,
               ["jobId", "name", "status", "submissionTime", "completionTime", "numTasks", "numCompletedTasks", "numFailedTasks", "stageIds"])
    _write_csv(outdir / "stages_summary.csv", stages,
               ["stageId", "attemptId", "name", "status", "numTasks", "submissionTime", "completionTime", "executorRunTime",
                "inputBytes", "inputRecords", "outputBytes", "shuffleReadBytes", "shuffleReadRecords", "shuffleWriteBytes",
                "shuffleWriteRecords", "memoryBytesSpilled", "diskBytesSpilled"])
    for r in sql:
        r["description"] = (r.get("description") or "").replace("\n", " ")[:200]
    _write_csv(outdir / "sql_summary.csv", sql, ["id", "status", "description", "duration", "runningJobIds", "successJobIds", "failedJobIds"])
    summary.update(jobs=len(jobs), stages=len(stages), sql_queries=len(sql))

    shots = 0
    if screenshots:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("[ui_capture] Chưa cài Playwright -> bỏ qua ảnh PNG (đã lưu JSON/CSV). "
                  "Cài: pip install playwright && python -m playwright install chromium")
            screenshots = False
    if screenshots:
        pages = [("jobs", f"{ui}/jobs/"), ("stages", f"{ui}/stages/"), ("storage", f"{ui}/storage/"),
                 ("environment", f"{ui}/environment/"), ("executors", f"{ui}/executors/"), ("sql", f"{ui}/SQL/")]
        seen = _SEEN.setdefault(app_id, {"sql": set(), "stage": set()})
        new_sql = [q for q in sorted(sql, key=lambda x: x["id"]) if q["id"] not in seen["sql"]][:max_sql]
        pages += [(f"sql_query_{q['id']:03d}", f"{ui}/SQL/execution/?id={q['id']}") for q in new_sql]
        new_stages = [st for st in stages if st["stageId"] not in seen["stage"]]
        longest = sorted(new_stages, key=lambda s: s.get("executorRunTime", 0), reverse=True)[:top_stages]
        seen["sql"].update(q["id"] for q in new_sql)
        seen["stage"].update(st["stageId"] for st in stages)
        pages += [(f"stage_{s['stageId']}", f"{ui}/stages/stage/?id={s['stageId']}&attempt={s.get('attemptId', 0)}") for s in longest]
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(executable_path=os.environ.get("CHROMIUM_PATH") or None)  # CHROMIUM_PATH: dùng Chrome/Chromium có sẵn
                page = browser.new_page(viewport={"width": 1600, "height": 1000})
                for name, url in pages:
                    try:
                        page.goto(url, wait_until="networkidle", timeout=45000)
                        page.wait_for_timeout(800)  # bảng DataTables vẽ xong
                        page.screenshot(path=str(outdir / f"{name}.png"), full_page=True)
                        shots += 1
                    except Exception as e:  # noqa: BLE001
                        print(f"[ui_capture] lỗi chụp {name}: {e}")
                browser.close()
        except Exception as e:  # noqa: BLE001
            print(f"[ui_capture] Playwright không chạy được ({e}). Nếu thiếu trình duyệt: python -m playwright install chromium")
    summary["screenshots"] = shots
    (outdir / "capture_info.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"[ui_capture] {tag}: {len(jobs)} job, {len(stages)} stage, {len(sql)} truy vấn SQL, {shots} ảnh -> {outdir}")
    return summary
