# import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Data Analysis").getOrCreate()

# Load data
api_df = spark.read.csv('s3://big-data-topicos/TRUSTED/api_db.csv',inferSchema=True,header=True)
local_df = spark.read.csv('s3://big-data-topicos/TRUSTED/local_db.csv',inferSchema=True,header=True)

# Reconocimiento del dataframe
def print_df_columns(df, df_name):
    print(f'{df_name} dataframe columns:')
    for column in df.columns:
        print(f' - {column}')
    print()
    
print_df_columns(api_df, 'Covid api data')
print_df_columns(local_df, 'Covid local data')

api_df.count()

local_df.count()

api_df.printSchema()

local_df.printSchema()

api_df.show()

local_df.show()

trusted_dir = "s3://big-data-topicos/REFINED/bussiness_analytics/dataframes"


from pyspark.sql.functions import col

# Total de casos por departamento y municipio
casos_por_ubicacion = api_df.groupBy("departamento_nombre", "municipio_nombre").count()
casos_por_ubicacion.orderBy(col("count").desc()).show()


casos_por_ubicacion.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "casos_por_ubicacion") 


from pyspark.sql.functions import to_date

# Convertir fechas a formato adecuado
df = api_df.withColumn("fecha_reporte", to_date("fecha_reporte", "yyyy-MM-dd"))

# Casos reportados por día
casos_por_dia = df.groupBy("fecha_reporte").count()
casos_por_dia.orderBy(col("count").desc()).show()

casos_por_dia.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "casos_por_dia") 


# Distribución de casos por edad
casos_por_edad = api_df.groupBy("edad").count()
casos_por_edad.orderBy(col("count").desc()).show()

casos_por_edad.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "casos_por_edad") 


# Distribución por sexo
casos_por_sexo = df.groupBy("sexo").count()
casos_por_sexo.show()

casos_por_sexo.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "casos_por_sexo") 


# Evaluar el porcentaje de recuperados por sexo
recuperacion_por_sexo = api_df.groupBy("sexo") \
    .agg(
        count(when(col("recuperado") == "Recuperado", True)).alias("casos_recuperados"),
        count("*").alias("total_casos")
    ) \
    .withColumn("porcentaje_recuperacion", (col("casos_recuperados") / col("total_casos")) * 100)
recuperacion_por_sexo.show()

recuperacion_por_sexo.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "recuperacion_por_sexo") 


from pyspark.sql.functions import when

# Filtrar casos recuperados
recuperados = df.filter(col("recuperado") == "Recuperado")

# Contar casos por departamento
tasa_recuperacion = recuperados.groupBy("departamento_nombre").count()
tasa_recuperacion.orderBy(col("count").desc()).show()

tasa_recuperacion.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "tasa_recuperacion_departamento") 


# Listar los municipios con mayor porcentae de muertes respecto al total de casos 
mortalidad_municipios = api_df.groupBy("municipio_nombre") \
    .agg(
        count(when(col("fecha_muerte") != "N/A", True)).alias("total_muertes"),
        count("*").alias("total_casos")
    ) \
    .withColumn("porcentaje_mortalidad", (col("total_muertes") / col("total_casos")) * 100) \
    .orderBy("porcentaje_mortalidad", ascending=False)
mortalidad_municipios.show()

mortalidad_municipios.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "mortalidad_municipios") 


from pyspark.sql.functions import avg,datediff

# Calcular días entre síntomas y diagnóstico
df = df.withColumn("dias_sintomas_diagnostico", datediff("fecha_diagnostico", "fecha_inicio_sintomas"))

# Calcular días entre diagnóstico y recuperación
df = df.withColumn("dias_diagnostico_recuperacion", datediff("fecha_recuperacion", "fecha_diagnostico"))

# Promedio de días
tiempos_promedio = df.select(
    avg("dias_sintomas_diagnostico").alias("promedio_dias_sintomas_diagnostico"),
    avg("dias_diagnostico_recuperacion").alias("promedio_dias_diagnostico_recuperacion")
)
tiempos_promedio.show()

tiempos_promedio.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "tiempos_promedio") 


# Promedio y mediana por municipio entre la fecha de diagnóstico y de recuperación

from pyspark.sql.window import Window
from pyspark.sql.functions import percentile_approx

# Calcular días de recuperación
api_df = api_df.withColumn("dias_recuperacion", 
    datediff(col("fecha_recuperacion"), col("fecha_inicio_sintomas")))

dias_recuperacion_estadisticas = api_df.groupBy("municipio_nombre") \
    .agg(
        avg("dias_recuperacion").alias("promedio_dias_recuperacion"),
        percentile_approx("dias_recuperacion", 0.5).alias("mediana_dias_recuperacion")
    ) \
    .orderBy("promedio_dias_recuperacion")
dias_recuperacion_estadisticas.show()

dias_recuperacion_estadisticas.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "estadisticas_recuperacion_municipios") 

from pyspark.sql.functions import col, avg, count, when, lit

# Unión de los DataFrames en base a fecha_reporte
combined_df = api_df.join(local_df, on="fecha_reporte", how="inner")

# Consulta 2: Relación entre tasa de transmisión y personas vacunadas
consulta_2 = combined_df.groupBy("tasa_transmision") \
    .agg(
        avg("personas_vacunadas").alias("promedio_vacunados"),
        count("*").alias("total_registros")
    ).orderBy(col("tasa_transmision"))
consulta_2.show()

consulta_2.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "transmision_vacunacion") 


# Consulta 3: Comparar casos recuperados con medidas de control
consulta_3 = combined_df.groupBy("medidas_control") \
    .agg(
        count(when(col("recuperado") == "Recuperado", True)).alias("total_recuperados"),
        count("*").alias("total_casos")
    ).withColumn(
        "porcentaje_recuperados",
        (col("total_recuperados") / col("total_casos")) * 100
    )
consulta_3.show()

consulta_3.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "medidas_control") 


# Consulta 4: Relación entre edad promedio y ocupación UCI
consulta_4 = combined_df.groupBy("fecha_reporte") \
    .agg(
        avg("edad").alias("edad_promedio"),
        avg("ocupacion_uci").alias("ocupacion_uci_promedio")
    ).orderBy(col("fecha_reporte"))
consulta_4.show()

consulta_4.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "edad_ocupacion_UCI") 


