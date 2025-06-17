from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import re

spark = SparkSession.builder.appName("InvertedIndexVIRAT").getOrCreate()

# Ruta en HDFS con los archivos JSON (cada línea JSON o archivos JSON múltiples)
input_path = "hdfs:///datos_planos_json/*"

# Leer los JSON en DataFrame (Spark intentará inferir el esquema)
df = spark.read.json(input_path)

# Explorar esquema para entender estructura
df.printSchema()

# Como tus JSON tienen dos secciones "meta" y "types"
# Queremos extraer:
# - camera_id: extraído de las keys en 'meta' que matchean VIRAT_S_\d+
# - De 'types' cada objeto con sus 'type' y 'confidence'

# Primero obtenemos el camera_id (la key en meta que matchee)
# Para simplificar, lo extraemos usando UDF o expresiones SQL. 
# Pero como meta es un mapa, tomamos la primera key que cumple patrón.

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def extract_camera_id(meta):
    if meta is None:
        return None
    for k in meta.keys():
        if re.match(r'VIRAT_S_\d+', k):
            return k
    return None

extract_camera_id_udf = udf(extract_camera_id, StringType())

df = df.withColumn("camera_id", extract_camera_id_udf(col("meta")))

# Ahora 'types' es un mapa de id->objeto, convertimos a array con explode para procesar

from pyspark.sql.functions import explode

df_types = df.select("camera_id", explode("types").alias("obj_id", "obj_info"))

# obj_info tiene campos 'type' y 'confidence'
df_types = df_types.select(
    "camera_id",
    col("obj_info.type").alias("obj_type"),
    col("obj_info.confidence").alias("confidence")
)

# Filtramos filas sin obj_type o sin camera_id (por si hay datos corruptos)
df_types = df_types.filter((col("obj_type").isNotNull()) & (col("camera_id").isNotNull()))

# Ahora agrupamos por obj_type para agregar las cámaras y confianza
from pyspark.sql.functions import collect_list, struct

df_grouped = df_types.groupBy("obj_type").agg(
    collect_list(
        struct("camera_id", "confidence")
    ).alias("camera_conf_list")
)

# Como en tu reduce: ordenar y eliminar duplicados por cámara dejando la mayor confianza
# Para eso creamos una función que procese la lista

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def format_camera_conf_list(camera_conf_series: pd.Series) -> pd.Series:
    results = []
    for camera_conf in camera_conf_series:
        # camera_conf es una lista de dicts con keys camera_id y confidence
        # Ordenamos por confidence desc y filtramos duplicados de cámara
        sorted_list = sorted(camera_conf, key=lambda x: x['confidence'], reverse=True)
        unique_cams = {}
        for entry in sorted_list:
            cam = entry['camera_id']
            conf = entry['confidence']
            if cam not in unique_cams:
                unique_cams[cam] = conf
        cam_list_str = ",".join([f"{cam}({conf:.1f})" for cam, conf in unique_cams.items()])
        results.append(cam_list_str)
    return pd.Series(results)

df_final = df_grouped.withColumn("cameras_conf", format_camera_conf_list(col("camera_conf_list")))

# Seleccionamos solo los resultados
df_result = df_final.select("obj_type", "cameras_conf")

# Guardamos en formato texto (obj_type \t lista_cams)
df_result.write.mode("overwrite").csv("hdfs:///output/indice_invertido_spark", sep="\t")

spark.stop()

