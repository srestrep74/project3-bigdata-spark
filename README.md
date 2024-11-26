# project3-bigdata-spark

## Descripción del Proyecto
Este proyecto automatiza la captura, ingesta, procesamiento y salida de datos accionables para la gestión de datos de Covid en Colombia. Utiliza una arquitectura batch para big data, empleando tecnologías como Apache Spark y AWS EMR.

## Estructura del Proyecto

```
/
├── df_visualization/       # Visualización de datos
├── notebooks/              # Notebooks de Jupyter
│   ├── Analytics.ipynb     # Análisis de datos
│   ├── cluster_creation.ipynb # Automatización de clúster EMR
│   ├── ETL.ipynb           # Proceso ETL
│   └── Sparksql.ipynb      # Consultas con SparkSQL
├── scripts/                # Scripts de Python
│   ├── dataframes.py       # Manipulación de dataframes
│   ├── ETL.py              # Proceso ETL automatizado
│   ├── ingest.py           # Ingesta de datos
│   ├── load_s3.py          # Carga de datos a S3
│   └── Sparksql.py         # Consultas con SparkSQL
└── venv/                   # Entorno virtual
```

## Requisitos Previos
- Apache Spark
- AWS CLI configurado
- Python 3.x
- Jupyter Notebook
- [Otras dependencias necesarias]


## Uso
1. **Automatización del Clúster EMR**: Ejecuta `cluster_creation.ipynb` para crear y configurar automáticamente un clúster EMR en AWS.
2. **Procesamiento de Datos y Automatización de ETL**: Ejecuta los scripts en la carpeta `scripts/` para automatizar procesos ETL y consultas con SparkSQL.

## Proceso Automatizado
El proyecto utiliza un script principal `cluster_creation.py` que automatiza todo el proceso de análisis de datos. Este script realiza las siguientes operaciones secuencialmente:

1. **Extracción de Datos**
   - Conecta con la API de datos.gov.co mediante Socrata
   - Extrae hasta 1,000,000 registros de datos COVID
   - Almacena los datos iniciales en un DataFrame de pandas

2. **Creación y Configuración del Cluster EMR**
   - Configura un cluster EMR con las siguientes características:
     - 1 nodo Master (m5.xlarge)
     - 2 nodos Core (m5.xlarge)
     - 1 nodo Task (m5.xlarge)
   - Instala aplicaciones necesarias: Spark, Hadoop, Hive, JupyterHub, etc.

3. **Pasos Automatizados del Proceso**
   El cluster ejecuta automáticamente los siguientes pasos:
   1. Instalación de dependencias Python necesarias
   2. Carga de datos a S3 (load_s3.py)
   3. Proceso ETL (ETL.py)
   4. Manipulación de DataFrames (dataframes.py)
   5. Consultas SparkSQL (Sparksql.py)
   6. Ejecución del Crawler para catalogar datos

## Ejecución del Proceso
Para ejecutar todo el proceso automatizado:

1. Asegúrate de tener configuradas las credenciales de AWS
2. Ejecuta el script principal:
   ```bash
   python scripts/cluster_creation.py
   ```
3. El proceso creará el cluster EMR y ejecutará todos los pasos automáticamente
4. Los resultados se almacenarán en el bucket S3 especificado

## Directorios en S3
- **RAW**: Datos crudos iniciales en S3
- **Processed**: Datos después del proceso ETL
- **Refined**: Resultados finales de análisis
- **EMR/logs**: Logs del proceso de ejecución

## Preguntas de Negocio Analizadas
1. **Distribución geográfica de casos**: Identificación de zonas de mayor incidencia.
2. **Evolución temporal de casos**: Identificación de patrones temporales de la pandemia.
3. **Distribución por edad**: Análisis de grupos etarios más afectados.
4. **Distribución por sexo**: Diferencias en el impacto del virus según el género.
5. **Tasa de recuperación por sexo**: Diferencias en la efectividad de recuperación entre géneros.
6. **Tasa de recuperación por departamento**: Éxito en la recuperación de pacientes por región.
7. **Mortalidad por municipio**: Municipios con tasas más altas de mortalidad.
8. **Tiempos de evolución de la enfermedad**: Intervalos típicos entre síntomas, diagnóstico y recuperación.
9. **Estadísticas de recuperación por municipio**: Variación en tiempos de recuperación por localización.
10. **Relación entre transmisión y vacunación**: Correlación entre tasas de transmisión y nivel de vacunación.
11. **Efectividad de medidas de control**: Impacto de medidas de control en la tasa de recuperación.
12. **Relación edad-ocupación UCI**: Correlación entre edad y necesidad de cuidados intensivos.

## Resultados
Los resultados del análisis de datos se almacenan en un bucket S3 en la zona Refined y pueden ser consultados mediante Athena o API Gateways.

## Video
https://www.youtube.com/watch?v=5EQA9WJoYGY



