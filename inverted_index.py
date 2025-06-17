from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, explode, col, collect_set

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("IndiceInvertidoVIRAT") \
    .getOrCreate()

# Leer todos los JSON desde HDFS
df = spark.read.json("hdfs:///datos_planos_json/*.json", multiLine=True)

# Extraer nombre del archivo como ID de video
df = df.withColumn("video_id", regexp_extract(input_file_name(), r"([^/]+)\.json$", 1))

# Explotar el campo "types" a pares (id, objeto)
df_exploded = df.select("video_id", explode("types").alias("type_id", "type_info"))

# Obtener pares (etiqueta, video)
video_labels = df_exploded.select(col("type_info.type").alias("label"), "video_id")

# Agrupar por etiqueta → índice invertido
inverted_index = video_labels.groupBy("label").agg(collect_set("video_id").alias("videos"))

# Guardar resultado como JSON
inverted_index.write.mode("overwrite").json("hdfs:///output/indice_invertido")

spark.stop()
