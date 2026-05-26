from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import xml.etree.ElementTree as ET
import os

RAW_PATH = "/opt/bitnami/spark/data/raw"
LAKEHOUSE = "/opt/bitnami/spark/lakehouse"
POWERBI = "/opt/bitnami/spark/powerbi"

spark = (
    SparkSession.builder
    .appName("RetailX - Data Lakehouse Medallion")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

os.makedirs(LAKEHOUSE, exist_ok=True)
os.makedirs(POWERBI, exist_ok=True)

print("Iniciando pipeline Data-Lakehouse RetailX...")

ventas_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{RAW_PATH}/ventas_csv")
)

clientes_raw = (
    spark.read
    .option("inferSchema", True)
    .json(f"{RAW_PATH}/clientes_json")
)

xml_path = f"{RAW_PATH}/gps_camiones.xml"
xml_rows = []

if os.path.exists(xml_path):
    root = ET.parse(xml_path).getroot()
    for evento in root.findall("evento"):
        xml_rows.append({
            "id_evento": evento.findtext("id_evento"),
            "id_camion": evento.findtext("id_camion"),
            "zona": evento.findtext("zona"),
            "estado": evento.findtext("estado"),
            "velocidad": evento.findtext("velocidad"),
            "toneladas": evento.findtext("toneladas"),
            "capacidad_patio": evento.findtext("capacidad_patio"),
            "timestamp": evento.findtext("timestamp")
        })

gps_schema = StructType([
    StructField("id_evento", StringType(), True),
    StructField("id_camion", StringType(), True),
    StructField("zona", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("velocidad", StringType(), True),
    StructField("toneladas", StringType(), True),
    StructField("capacidad_patio", StringType(), True),
    StructField("timestamp", StringType(), True),
])

gps_raw = spark.createDataFrame(xml_rows, gps_schema)

ventas_raw.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/bronze/ventas")
clientes_raw.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/bronze/clientes")
gps_raw.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/bronze/gps_camiones")

print("Capa Bronze creada.")

ventas_silver = (
    ventas_raw
    .dropDuplicates(["id_venta"])
    .withColumn("id_venta", F.col("id_venta").cast(IntegerType()))
    .withColumn("id_cliente", F.col("id_cliente").cast(IntegerType()))
    .withColumn("id_producto", F.col("id_producto").cast(IntegerType()))
    .withColumn("cantidad", F.col("cantidad").cast(IntegerType()))
    .withColumn("precio_unitario", F.col("precio_unitario").cast(DoubleType()))
    .withColumn("descuento", F.col("descuento").cast(DoubleType()))
    .withColumn("monto", F.col("monto").cast(DoubleType()))
    .withColumn("fecha", F.to_date("fecha"))
    .filter(F.col("id_cliente").isNotNull())
    .filter(F.col("monto") > 0)
)

clientes_silver = (
    clientes_raw
    .dropDuplicates(["id_cliente"])
    .withColumn("id_cliente", F.col("id_cliente").cast(IntegerType()))
    .withColumn("antiguedad_meses", F.col("antiguedad_meses").cast(IntegerType()))
    .fillna({"segmento": "Sin segmento", "ciudad": "Sin ciudad"})
)

ventas_enriquecidas = (
    ventas_silver.alias("v")
    .join(clientes_silver.alias("c"), on="id_cliente", how="left")
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
)

gps_silver = (
    gps_raw
    .dropDuplicates(["id_evento"])
    .withColumn("id_evento", F.col("id_evento").cast(IntegerType()))
    .withColumn("velocidad", F.col("velocidad").cast(DoubleType()))
    .withColumn("toneladas", F.col("toneladas").cast(DoubleType()))
    .withColumn("capacidad_patio", F.col("capacidad_patio").cast(IntegerType()))
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .filter(F.col("id_camion").isNotNull())
)

ventas_enriquecidas.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/silver/ventas_enriquecidas")
gps_silver.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/silver/gps_camiones_limpio")

print("Capa Silver creada.")

gold_cliente = (
    ventas_enriquecidas
    .groupBy("id_cliente", "segmento", "ciudad")
    .agg(
        F.round(F.sum("monto"), 2).alias("total_compra"),
        F.count("*").alias("cantidad_compras"),
        F.round(F.avg("monto"), 2).alias("ticket_promedio")
    )
    .orderBy(F.desc("total_compra"))
)

gold_ventas_mensuales = (
    ventas_enriquecidas
    .groupBy("anio", "mes", "canal")
    .agg(
        F.round(F.sum("monto"), 2).alias("ventas_totales"),
        F.count("*").alias("transacciones"),
        F.round(F.avg("monto"), 2).alias("ticket_promedio")
    )
    .orderBy("anio", "mes", "canal")
)

gold_top_productos = (
    ventas_enriquecidas
    .groupBy("id_producto")
    .agg(
        F.round(F.sum("monto"), 2).alias("ventas_totales"),
        F.sum("cantidad").alias("unidades_vendidas")
    )
    .orderBy(F.desc("ventas_totales"))
)

gold_gps_resumen = (
    gps_silver
    .groupBy("zona", "estado")
    .agg(
        F.countDistinct("id_camion").alias("camiones"),
        F.round(F.sum("toneladas"), 2).alias("toneladas_transportadas"),
        F.round(F.avg("velocidad"), 2).alias("velocidad_promedio"),
        F.round(F.avg("capacidad_patio"), 2).alias("capacidad_patio_promedio")
    )
    .orderBy("zona", "estado")
)

gold_cliente.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/gold/clientes")
gold_ventas_mensuales.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/gold/ventas_mensuales")
gold_top_productos.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/gold/top_productos")
gold_gps_resumen.write.format("delta").mode("overwrite").save(f"{LAKEHOUSE}/gold/gps_resumen")

gold_cliente.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{POWERBI}/gold_clientes")
gold_ventas_mensuales.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{POWERBI}/gold_ventas_mensuales")
gold_top_productos.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{POWERBI}/gold_top_productos")
gold_gps_resumen.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{POWERBI}/gold_gps_resumen")

print("Capa Gold creada y exportada para Power BI.")
print("Top 10 clientes por volumen de compra:")
gold_cliente.show(10, truncate=False)

print("Ventas mensuales por canal:")
gold_ventas_mensuales.show(20, truncate=False)

print("Resumen GPS:")
gold_gps_resumen.show(20, truncate=False)

spark.stop()