import importlib, shutil, sys

print("Python:", sys.version.split()[0])
for m in ["requests", "pandas", "pyspark", "kafka", "streamlit", "plotly", "pyarrow"]:
    try:
        mod = importlib.import_module(m)
        print("OK     ", m, getattr(mod, "__version__", ""))
    except Exception:
        print("MISSING", m)

print("java  :", shutil.which("java") or "NOT FOUND")
print("docker:", shutil.which("docker") or "NOT FOUND")
print("git   :", shutil.which("git") or "NOT FOUND")

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("check").getOrCreate()
print("Spark test count (expect 5):", spark.range(5).count())
spark.range(5).write.mode("overwrite").parquet("output/_test_parquet")
print("Parquet write: OK")
spark.stop()