from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "gpscamiones"
LAKEHOUSE = "/opt/bitnami/spark/lakehouse"
CHECKPOINTS = "/opt/bitnami/spark/checkpoints"
POWERBI = "/opt/bitnami/spark/powerbi"

spark = (
    SparkSession.builder
    .appName("RetailX - Real Time KPIs")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id_camion", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("zona", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("velocidad", DoubleType(), True),
    StructField("toneladas", DoubleType(), True),
    StructField("capacidad_patio", IntegerType(), True)
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

eventos = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
    .withWatermark("event_time", "1 minute")
)

kpis = (
    eventos
    .groupBy(F.window("event_time", "30 seconds"), "zona")
    .agg(
        F.approx_count_distinct("id_camion").alias("camiones_activos"),
        F.round(F.sum("toneladas"), 2).alias("toneladas_transportadas"),
        F.round(F.avg("velocidad"), 2).alias("velocidad_promedio"),
        F.round(F.avg("capacidad_patio"), 2).alias("capacidad_patio_promedio"),
        F.count("*").alias("eventos_recibidos"),
        F.sum(
            F.when(
                (F.col("estado") == "RETRASADO") | (F.col("velocidad") < 20),
                1
            ).otherwise(0)
        ).alias("alertas_retraso")
    )
    .select(
        F.col("window.start").alias("inicio_ventana"),
        F.col("window.end").alias("fin_ventana"),
        "zona",
        "camiones_activos",
        "toneladas_transportadas",
        "velocidad_promedio",
        "capacidad_patio_promedio",
        "eventos_recibidos",
        "alertas_retraso"
    )
)

def guardar_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    salida = (
        batch_df
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("procesado_en", F.current_timestamp())
    )

    print(f"\n========== KPIs EN VIVO - Batch {batch_id} ==========")
    salida.orderBy(F.desc("inicio_ventana"), "zona").show(50, truncate=False)

    (
        salida.write
        .format("delta")
        .mode("append")
        .save(f"{LAKEHOUSE}/gold/streaming_kpis")
    )

    (
        salida.coalesce(1)
        .write
        .mode("append")
        .option("header", True)
        .csv(f"{POWERBI}/streaming_kpis")
    )

query = (
    kpis.writeStream
    .outputMode("complete")
    .foreachBatch(guardar_batch)
    .option("checkpointLocation", f"{CHECKPOINTS}/streaming_kpis")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming iniciado. Presiona CTRL + C para detener.")
query.awaitTermination()