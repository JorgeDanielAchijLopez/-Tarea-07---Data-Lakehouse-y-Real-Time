from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import random
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

BASE_DATA = "/opt/bitnami/spark/data"
RAW_PATH = f"{BASE_DATA}/raw"

spark = (
    SparkSession.builder
    .appName("RetailX - Generacion de Fuentes")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

os.makedirs(RAW_PATH, exist_ok=True)

print("Generando dataset de 1,000,000 ventas para RetailX...")

ventas = (
    spark.range(1, 1_000_001)
    .withColumnRenamed("id", "id_venta")
    .withColumn("id_cliente", (F.floor(F.rand(seed=10) * 10000) + 1).cast("int"))
    .withColumn("id_producto", (F.floor(F.rand(seed=20) * 500) + 1).cast("int"))
    .withColumn("cantidad", (F.floor(F.rand(seed=30) * 5) + 1).cast("int"))
    .withColumn("precio_unitario", F.round((F.rand(seed=40) * 490) + 10, 2))
    .withColumn("descuento", F.round(F.rand(seed=50) * 0.20, 2))
    .withColumn(
        "fecha",
        F.date_sub(F.current_date(), F.floor(F.rand(seed=60) * 365).cast("int"))
    )
    .withColumn(
        "canal",
        F.expr(
            """
            CASE
              WHEN rand(70) < 0.40 THEN 'POS'
              WHEN rand(71) < 0.70 THEN 'WEB'
              WHEN rand(72) < 0.90 THEN 'APP'
              ELSE 'MARKETPLACE'
            END
            """
        )
    )
    .withColumn("tienda", F.concat(F.lit("Tienda-"), (F.floor(F.rand(seed=80) * 25) + 1).cast("int")))
    .withColumn("pais", F.lit("Guatemala"))
    .withColumn("monto", F.round(F.col("cantidad") * F.col("precio_unitario") * (1 - F.col("descuento")), 2))
)

ventas.write.mode("overwrite").option("header", True).csv(f"{RAW_PATH}/ventas_csv")

print("Generando clientes en JSON...")

clientes = (
    spark.range(1, 10001)
    .withColumnRenamed("id", "id_cliente")
    .withColumn(
        "segmento",
        F.expr(
            """
            CASE
              WHEN rand(11) < 0.25 THEN 'Premium'
              WHEN rand(12) < 0.60 THEN 'Regular'
              WHEN rand(13) < 0.85 THEN 'Nuevo'
              ELSE 'Mayorista'
            END
            """
        )
    )
    .withColumn(
        "ciudad",
        F.expr(
            """
            CASE
              WHEN rand(21) < 0.35 THEN 'Guatemala'
              WHEN rand(22) < 0.55 THEN 'Quetzaltenango'
              WHEN rand(23) < 0.75 THEN 'Huehuetenango'
              WHEN rand(24) < 0.90 THEN 'Escuintla'
              ELSE 'Petén'
            END
            """
        )
    )
    .withColumn("antiguedad_meses", (F.floor(F.rand(seed=90) * 72) + 1).cast("int"))
)

clientes.write.mode("overwrite").json(f"{RAW_PATH}/clientes_json")

print("Generando telemetria IoT/GPS en XML...")

root = ET.Element("telemetria")
zonas = ["NORTE", "SUR", "CENTRO", "OCCIDENTE", "ORIENTE"]
estados = ["ACTIVO", "ACTIVO", "ACTIVO", "RETRASADO", "MANTENIMIENTO"]

for i in range(1, 501):
    evento = ET.SubElement(root, "evento")
    ET.SubElement(evento, "id_evento").text = str(i)
    ET.SubElement(evento, "id_camion").text = f"CAM-{random.randint(1, 100):03d}"
    ET.SubElement(evento, "zona").text = random.choice(zonas)
    ET.SubElement(evento, "estado").text = random.choice(estados)
    ET.SubElement(evento, "velocidad").text = str(round(random.uniform(5, 90), 2))
    ET.SubElement(evento, "toneladas").text = str(round(random.uniform(1, 25), 2))
    ET.SubElement(evento, "capacidad_patio").text = str(random.randint(40, 100))
    ET.SubElement(evento, "timestamp").text = (
        datetime.now() - timedelta(minutes=random.randint(1, 1440))
    ).strftime("%Y-%m-%d %H:%M:%S")

tree = ET.ElementTree(root)
tree.write(f"{RAW_PATH}/gps_camiones.xml", encoding="utf-8", xml_declaration=True)

print("Fuentes generadas correctamente:")
print(f"- CSV ventas: {RAW_PATH}/ventas_csv")
print(f"- JSON clientes: {RAW_PATH}/clientes_json")
print(f"- XML GPS: {RAW_PATH}/gps_camiones.xml")

spark.stop()