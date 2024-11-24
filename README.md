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

## Instalación
1. Clona el repositorio:
   ```bash
   git clone https://github.com/tu_usuario/project3-bigdata-spark.git
   ```
2. Configura el entorno virtual:
   ```bash
   cd project3-bigdata-spark
   python -m venv venv
   source venv/bin/activate  # En Windows usa `venv\Scripts\activate`
   pip install -r requirements.txt
   ```

## Uso
1. **Automatización del Clúster EMR**: Ejecuta `cluster_creation.ipynb` para crear y configurar automáticamente un clúster EMR en AWS.
2. **Procesamiento de Datos**: Usa los notebooks en la carpeta `notebooks/` para realizar análisis y procesamiento de datos.
3. **Automatización de ETL**: Ejecuta los scripts en la carpeta `scripts/` para automatizar procesos ETL y consultas.

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



