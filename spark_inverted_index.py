import json
import os
from pyspark import SparkContext

def process_file(file_path, content):
    """Procesa el contenido de un archivo línea por línea"""
    lines = content.splitlines()
    results = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Determinar el doc_id (misma lógica que el mapper original)
        meta = record.get('meta', {})
        doc_ids = [k for k in meta if k != 'types' and k.lower() not in ['other', 'person', 'vehicle']]
        
        if doc_ids:
            doc_id = doc_ids[0]
        else:
            # Usar el nombre del archivo como fallback
            doc_id = os.path.basename(file_path)

        # Procesar los tipos de objetos (misma lógica que el mapper original)
        types = record.get('types', {})
        for obj_id, info in types.items():
            obj_type = info.get('type')
            if obj_type:
                results.append((obj_type, doc_id))
                
    return results

if __name__ == "__main__":
    from pyspark import SparkConf

    # Configurar Spark
    conf = SparkConf().setAppName("InvertedIndexSpark")
    sc = SparkContext(conf=conf)

    # Rutas de entrada y salida
    input_path = "hdfs://ruta/a/tu/input"  # Reemplazar con tu ruta real
    output_path = "hdfs://ruta/a/tu/output"  # Reemplazar con tu ruta real

    try:
        # Leer todos los archivos (similar a Hadoop input)
        docs_rdd = sc.wholeTextFiles(input_path)

        # Mapear cada línea a pares (obj_type, doc_id)
        mapped_rdd = docs_rdd.flatMap(lambda x: process_file(x[0], x[1]))

        # Reducir por clave (similar al reducer original)
        reduced_rdd = mapped_rdd.groupByKey().mapValues(
            lambda vals: sorted(set(vals))  # Eliminar duplicados y ordenar
        )

        # Formatear la salida en formato: "obj_type\tdoc1,doc2,..."
        formatted_rdd = reduced_rdd.map(
            lambda x: f"{x[0]}\t{','.join(x[1])}"
        )

        # Guardar el resultado
        formatted_rdd.saveAsTextFile(output_path)

    finally:
        # Asegurarse de detener el contexto de Spark
        sc.stop()
