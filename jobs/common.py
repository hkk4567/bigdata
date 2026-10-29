"""Cấu hình Spark dùng chung cho mọi job (tối ưu cho máy ~8 GB RAM)."""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCAL_HADOOP = ROOT / "tools" / "hadoop"


def results_dir(script_file: str, override=None) -> Path:
    """Thư mục kết quả riêng của từng script: results/<tên-script-không-đuôi>/ (tự tạo)."""
    d = Path(override) if override else ROOT / "results" / Path(script_file).stem
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_windows_hadoop():
    """Windows: Spark cần winutils.exe. Dùng bản trong tools/hadoop (tạo bằng scripts/setup_windows_hadoop.py)."""
    if os.name != "nt":
        return
    home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if home and (Path(home) / "bin" / "winutils.exe").exists():
        return  # người dùng đã tự cấu hình
    if (LOCAL_HADOOP / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(LOCAL_HADOOP)
        os.environ["PATH"] = str(LOCAL_HADOOP / "bin") + os.pathsep + os.environ.get("PATH", "")
        return
    sys.exit(
        "THIẾU winutils.exe (Windows). Chạy:\n"
        "    python scripts/setup_windows_hadoop.py\n"
        "rồi chạy lại job này. (Hoặc dùng WSL2/Docker – xem README.)"
    )


def ensure_working_java():
    """JAVA_HOME trỏ tới bản Java hỏng (vd. Android Studio cài dở: thiếu jvm.cfg) làm Spark không khởi động được,
    dù `java` trong PATH vẫn tốt. Nếu java của JAVA_HOME không chạy được thì bỏ JAVA_HOME (Spark sẽ dùng java trong PATH)."""
    home = os.environ.get("JAVA_HOME")
    if not home:
        return
    exe = Path(home) / "bin" / ("java.exe" if os.name == "nt" else "java")
    try:
        ok = exe.exists() and subprocess.run([str(exe), "-version"], capture_output=True, timeout=30).returncode == 0
    except Exception:  # noqa: BLE001
        ok = False
    if not ok:
        print(f"[CẢNH BÁO] JAVA_HOME={home} không chạy được -> bỏ qua JAVA_HOME, dùng java trong PATH. "
              "Nên sửa/xoá biến JAVA_HOME trong Environment Variables.", file=sys.stderr)
        os.environ.pop("JAVA_HOME", None)


def get_spark(app_name: str, cores: int = 2, driver_mem: str = "3g", shuffle_partitions: int = 16):
    ensure_windows_hadoop()  # phải chạy trước khi JVM khởi động
    ensure_working_java()
    # Dùng đúng Python hiện tại cho driver và worker (tránh lỗi lệch phiên bản).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
