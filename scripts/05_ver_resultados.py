from pyspark.sql import SparkSession
import os

LAKEHOUSE = "/opt/bitnami/spark/lakehouse"

spark = (
    SparkSession.builder
    .appName("RetailX - Ver Resultados")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def mostrar_tabla(nombre, ruta, limite=10):
    print(f"\n========== {nombre} ==========")
    if os.path.exists(ruta):
        df = spark.read.format("delta").load(ruta)
        print(f"Registros aproximados: {df.count()}")
        df.show(limite, truncate=False)
    else:
        print(f"No existe todavía: {ruta}")

mostrar_tabla("BRONZE - Ventas crudas", f"{LAKEHOUSE}/bronze/ventas", 5)
mostrar_tabla("SILVER - Ventas enriquecidas", f"{LAKEHOUSE}/silver/ventas_enriquecidas", 5)
mostrar_tabla("GOLD - Clientes", f"{LAKEHOUSE}/gold/clientes", 10)
mostrar_tabla("GOLD - Ventas mensuales", f"{LAKEHOUSE}/gold/ventas_mensuales", 20)
mostrar_tabla("GOLD - Top productos", f"{LAKEHOUSE}/gold/top_productos", 10)
mostrar_tabla("GOLD - GPS resumen", f"{LAKEHOUSE}/gold/gps_resumen", 20)
mostrar_tabla("GOLD - Streaming KPIs", f"{LAKEHOUSE}/gold/streaming_kpis", 20)

print("\nRutas generadas:")
print("- Lakehouse:", LAKEHOUSE)
print("- Power BI CSV:", "/opt/bitnami/spark/powerbi")
print("- Spark UI:", "http://localhost:8080")

spark.stop()