
# Explicación `spark_inverted_index.py`

Este script crea un índice invertido utilizando Apache Spark a partir de archivos JSON. Un índice invertido asocia cada término (en este caso, tipos de objetos) con los documentos en los que aparece, facilitando búsquedas eficientes. A continuación, se detalla el funcionamiento del código:

---

## 1. Importaciones y Definición Principal

- **pyspark.sql:** Para manejar datos distribuidos.
- **json:** Para procesar líneas de texto como objetos JSON.

---

## 2. Función `create_inverted_index`

### Parámetros

- `input_path`: Ruta de los archivos JSON de entrada.
- `output_path`: Ruta donde guardar el índice invertido.

### Proceso

1. **Inicializa una SparkSession** optimizada para procesamiento distribuido.
2. **Lee los archivos JSON** como texto plano, una línea por registro.
3. **Procesa cada línea** mediante la función `process_json_line`:
    - Intenta cargar la línea como JSON.
    - Busca el `document_id` en la clave `meta` (omitiendo ciertos valores).
    - Para cada objeto en `types`, extrae el tipo (`type`) y lo asocia con el documento.
4. **Crea un DataFrame** con los pares (tipo de objeto, documento).
5. **Agrupa por tipo de objeto** y junta los IDs de documentos asociados, ordenados y sin repetir.
6. **Guarda el índice** en formato CSV (separado por tabulador).
7. **Muestra estadísticas** como número de tipos únicos, pares totales y distribución de tipos.

---

## 3. Función `create_inverted_index_with_confidence`

Versión extendida que considera un umbral de confianza (`confidence`):

- Filtra los objetos por un valor mínimo de confianza.
- Calcula estadísticas adicionales: confianza promedio por tipo y cantidad de instancias.
- Guarda dos versiones del índice: una extendida (con estadísticas) y otra simple.

---

## 4. Función Principal (`main`)

Permite ejecutar el script desde línea de comandos con los siguientes argumentos:

- Ruta de entrada y salida.
- Umbral de confianza.
- Opción para incluir estadísticas adicionales.
- Modo verbose.

Escoge la función adecuada según los argumentos proporcionados.

---

## 5. Ejecución

Al ejecutar el script, procesa los archivos JSON y genera el índice invertido en la ruta de salida, con o sin estadísticas según se indique.

---

## Resumen visual del flujo principal

1. Lee archivos JSON línea por línea.
2. Extrae para cada línea los pares (tipo de objeto, documento), opcionalmente filtrando por confianza.
3. Agrupa los documentos por tipo de objeto.
4. Guarda el índice invertido y muestra estadísticas útiles.

---

¿Te gustaría ver un ejemplo de entrada y salida para este script?





## Explicación del archivo `spark_inverted_index.py`

El archivo `spark_inverted_index.py` implementa un índice invertido utilizando Apache Spark, una herramienta poderosa para el procesamiento distribuido de grandes volúmenes de datos. Un índice invertido es una estructura de datos ampliamente utilizada en motores de búsqueda, que permite mapear cada palabra de un conjunto de documentos a la lista de documentos donde aparece.

### ¿Cómo funciona el código?

1. **Lectura de los datos**:  
   El script comienza leyendo un conjunto de archivos (o un archivo grande) que contiene los documentos a analizar. Usualmente, cada línea representa un documento, aunque puede adaptarse según el formato de entrada.

2. **Procesamiento con Spark**:  
   Utilizando la API de Spark, el código distribuye la carga de trabajo entre varios nodos o núcleos disponibles. Esto permite procesar grandes volúmenes de texto de manera eficiente y rápida.

3. **Tokenización y limpieza**:  
   Cada documento se divide en palabras (tokens), eliminando caracteres especiales y normalizando el texto (por ejemplo, pasando todo a minúsculas). Esto asegura que las búsquedas sean consistentes y robustas.

4. **Creación del índice invertido**:  
   Para cada palabra encontrada en los documentos, se almacena una referencia al documento donde aparece. El resultado final es una estructura donde cada palabra clave apunta a una lista de identificadores de documentos.

5. **Almacenamiento del resultado**:  
   El índice invertido generado se guarda en un archivo o base de datos, listo para ser consultado por otros sistemas o aplicaciones.

### Ejemplo de uso

Supón que tienes un conjunto de artículos y quieres saber en cuáles aparece la palabra "Spark". El índice invertido permite buscar rápidamente "Spark" y obtener una lista de todos los documentos relevantes, sin tener que escanear cada documento uno por uno.

### Ventajas de usar Spark

- **Escalabilidad**: Procesa fácilmente grandes cantidades de datos.
- **Rapidez**: El procesamiento distribuido acelera la creación del índice.
- **Flexibilidad**: Se puede adaptar a distintos formatos y necesidades, modificando el código según el caso.

---

Este enfoque es ideal para proyectos de análisis de texto, motores de búsqueda, o cualquier aplicación que requiera búsquedas rápidas sobre grandes colecciones de documentos.
