#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_inverted_index(input_path, output_path):
    """
    Crea un índice invertido usando Apache Spark para JSONs aplanados
    Args:
        input_path: Ruta de los archivos JSON de entrada
        output_path: Ruta donde guardar el índice invertido
    """
    
    # Inicializar SparkSession
    spark = SparkSession.builder \
        .appName("InvertedIndexCreator") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        print("Iniciando procesamiento del índice invertido...")
        
        # Leer archivos JSON línea por línea (cada línea es un JSON completo)
        df_raw = spark.read.text(input_path)
        
        print(f"Líneas cargadas: {df_raw.count()}")
        
        def process_json_line(line):
            """
            Procesa cada línea JSON y extrae pares (object_type, document_id)
            Replica la lógica del mapper original
            """
            if not line or not line.strip():
                return []
            
            try:
                record = json.loads(line.strip())
            except:
                return []
            
            results = []
            
            # Determinar document_id desde meta (excluyendo 'types' y ciertos valores)
            meta = record.get('meta', {})
            doc_ids = [k for k in meta.keys() 
                      if k != 'types' and k.lower() not in ['other', 'person', 'vehicle']]
            
            if doc_ids:
                doc_id = doc_ids[0]
            else:
                doc_id = 'unknown'
            
            # Procesar cada objeto en 'types'
            types = record.get('types', {})
            for obj_id, info in types.items():
                obj_type = info.get('type')
                if obj_type:
                    results.append((obj_type, doc_id))
            
            return results
        
        # Aplicar la función de procesamiento usando flatMap
        pairs_rdd = df_raw.rdd.map(lambda row: row.value).flatMap(process_json_line)
        
        # Convertir a DataFrame
        schema = StructType([
            StructField("object_type", StringType(), True),
            StructField("document_id", StringType(), True)
        ])
        
        df_pairs = spark.createDataFrame(pairs_rdd, schema)
        
        print("Creando índice invertido...")
        
        # Crear el índice invertido (replica la lógica del reducer)
        inverted_index = df_pairs.groupBy("object_type") \
            .agg(collect_set("document_id").alias("document_list")) \
            .withColumn("documents", concat_ws(",", sort_array("document_list"))) \
            .select("object_type", "documents")
        
        print("Índice invertido creado. Guardando resultados...")
        
        # Mostrar algunos resultados
        print("\nPrimeros resultados del índice invertido:")
        inverted_index.show(20, truncate=False)
        
        # Guardar el resultado en formato similar al reducer original
        # Formato: object_type\tdocument1,document2,document3
        inverted_index.write \
            .mode("overwrite") \
            .option("header", "false") \
            .option("delimiter", "\t") \
            .option("quote", "") \
            .csv(output_path)
        
        print(f"Índice invertido guardado en: {output_path}")
        
        # Estadísticas finales
        total_types = inverted_index.count()
        total_pairs = df_pairs.count()
        unique_docs = df_pairs.select("document_id").distinct().count()
        
        print(f"\nEstadísticas:")
        print(f"- Tipos de objetos únicos: {total_types}")
        print(f"- Total de pares (tipo, documento): {total_pairs}")
        print(f"- Documentos únicos procesados: {unique_docs}")
        
        # Mostrar distribución de tipos
        print(f"\nDistribución por tipo de objeto:")
        type_counts = df_pairs.groupBy("object_type").count().orderBy(desc("count"))
        type_counts.show()
        
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")
        raise e
    
    finally:
        spark.stop()

def create_inverted_index_with_confidence(input_path, output_path, min_confidence=0.0):
    """
    Versión alternativa que también considera el confidence threshold
    """
    spark = SparkSession.builder \
        .appName("InvertedIndexWithConfidence") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        df_raw = spark.read.text(input_path)
        
        def process_json_with_confidence(line):
            if not line or not line.strip():
                return []
            
            try:
                record = json.loads(line.strip())
            except:
                return []
            
            results = []
            
            # Determinar document_id
            meta = record.get('meta', {})
            doc_ids = [k for k in meta.keys() 
                      if k != 'types' and k.lower() not in ['other', 'person', 'vehicle']]
            
            doc_id = doc_ids[0] if doc_ids else 'unknown'
            
            # Procesar con filtro de confidence
            types = record.get('types', {})
            for obj_id, info in types.items():
                obj_type = info.get('type')
                confidence = float(info.get('confidence', 0))
                
                if obj_type and confidence >= min_confidence:
                    results.append((obj_type, doc_id, confidence))
            
            return results
        
        # Procesar con confidence
        pairs_rdd = df_raw.rdd.map(lambda row: row.value).flatMap(process_json_with_confidence)
        
        schema = StructType([
            StructField("object_type", StringType(), True),
            StructField("document_id", StringType(), True),
            StructField("confidence", FloatType(), True)
        ])
        
        df_pairs = spark.createDataFrame(pairs_rdd, schema)
        
        # Crear índice con información de confidence promedio
        inverted_index = df_pairs.groupBy("object_type") \
            .agg(
                collect_set("document_id").alias("document_list"),
                avg("confidence").alias("avg_confidence"),
                count("document_id").alias("total_instances")
            ) \
            .withColumn("documents", concat_ws(",", sort_array("document_list"))) \
            .select("object_type", "documents", "avg_confidence", "total_instances")
        
        inverted_index.show(truncate=False)
        
        # Guardar resultado extendido
        inverted_index.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path + "_with_stats")
        
        # Guardar formato simple también
        inverted_index.select("object_type", "documents").write \
            .mode("overwrite") \
            .option("header", "false") \
            .option("delimiter", "\t") \
            .csv(output_path)
            
    finally:
        spark.stop()

def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Crear índice invertido con Apache Spark')
    parser.add_argument('input_path', help='Ruta de los archivos JSON de entrada')
    parser.add_argument('output_path', help='Ruta donde guardar el índice invertido')
    parser.add_argument('--confidence', '-c', type=float, default=0.0, 
                       help='Confidence mínimo para incluir objetos (default: 0.0)')
    parser.add_argument('--with-stats', action='store_true', 
                       help='Incluir estadísticas de confidence')
    parser.add_argument('--verbose', '-v', action='store_true', help='Modo verbose')
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"Ruta de entrada: {args.input_path}")
        print(f"Ruta de salida: {args.output_path}")
        print(f"Confidence mínimo: {args.confidence}")
    
    if args.with_stats:
        create_inverted_index_with_confidence(args.input_path, args.output_path, args.confidence)
    else:
        create_inverted_index(args.input_path, args.output_path)

if __name__ == "__main__":
    main()
