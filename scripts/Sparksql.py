spark

sc

spark.sql(
"""
CREATE OR REPLACE TEMPORARY VIEW api_df 
USING csv 
OPTIONS (path 's3://big-data-topicos/TRUSTED/local_db.csv', header 'true', inferSchema 'true');

"""
)

spark.sql(
"""
CREATE OR REPLACE TEMPORARY VIEW local_df 
USING csv 
OPTIONS (path 's3://big-data-topicos/TRUSTED/api_db.csv', header 'true', inferSchema 'true');
"""
)

def print_df_columns(view_name, df_name):
    print(f'{df_name} dataframe columns:')
    columns = spark.sql(f"DESCRIBE {view_name}").collect()
    for column in columns:
        print(f' - {column[0]}')
    print()

print_df_columns("api_df", "Covid API Data")
print_df_columns("local_df", "Covid Local Data")

count_query = spark.sql("SELECT COUNT(*) FROM api_df")
count_result = count_query.collect()[0][0]
print(f"El número total de filas en api_df es: {count_result}")


count_query = spark.sql("SELECT COUNT(*) FROM local_df")
count_result = count_query.collect()[0][0]
print(f"El número total de filas en local_df es: {count_result}")


columns = spark.sql("DESCRIBE api_df").collect()
print("api_df schema:")
for column in columns:
    print(f' - {column[0]}: {column[1]}')


columns = spark.sql("DESCRIBE local_df").collect()
print("local_df schema:")
for column in columns:
    print(f' - {column[0]}: {column[1]}')


spark.sql("SELECT * FROM api_df LIMIT 20").show()


spark.sql("SELECT * FROM local_df LIMIT 20").show()


trusted_dir = "s3://project-buck3/REFINED/spark/"

# Crear una vista temporal casos_por_ubicacion con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW casos_por_ubicacion AS
SELECT departamento_nombre, municipio_nombre, COUNT(*) as count
FROM api_df
GROUP BY departamento_nombre, municipio_nombre
ORDER BY count DESC
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM casos_por_ubicacion LIMIT 20").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM casos_por_ubicacion").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "casos_por_ubicacion")


spark.sql("""
CREATE OR REPLACE TEMP VIEW df_converted AS
SELECT *, to_date(fecha_reporte, 'yyyy-MM-dd') as fecha_reporte_converted
FROM api_df
""")
# Crear una vista temporal casos_por_dia con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW casos_por_dia AS
SELECT fecha_reporte_converted AS fecha_reporte, COUNT(*) as count
FROM df_converted
GROUP BY fecha_reporte_converted
ORDER BY count DESC
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM casos_por_dia LIMIT 20").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM casos_por_dia").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/casos_por_dia")


# Crear una vista temporal casos_por_edad con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW casos_por_edad AS
SELECT edad, COUNT(*) as count
FROM api_df
GROUP BY edad
ORDER BY count DESC
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM casos_por_edad LIMIT 20").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM casos_por_edad").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/casos_por_edad")
# Crear una vista temporal casos_por_sexo con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW casos_por_sexo AS
SELECT sexo, COUNT(*) as count
FROM api_df
GROUP BY sexo
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM casos_por_sexo").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM casos_por_sexo").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/casos_por_sexo")


# Crear una vista temporal recuperacion_por_sexo con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW recuperacion_por_sexo AS
SELECT sexo,
       COUNT(CASE WHEN recuperado = 'Recuperado' THEN 1 END) as casos_recuperados,
       COUNT(*) as total_casos,
       (COUNT(CASE WHEN recuperado = 'Recuperado' THEN 1 END) / COUNT(*)) * 100 as porcentaje_recuperacion
FROM api_df
GROUP BY sexo
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM recuperacion_por_sexo").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM recuperacion_por_sexo").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/recuperacion_por_sexo")


# Crear una vista temporal recuperados con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW recuperados AS
SELECT *
FROM api_df
WHERE recuperado = 'Recuperado'
""")
# Crear una vista temporal tasa_recuperacion con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW tasa_recuperacion AS
SELECT departamento_nombre, COUNT(*) as count
FROM recuperados
GROUP BY departamento_nombre
ORDER BY count DESC
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM tasa_recuperacion LIMIT 20").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM tasa_recuperacion").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/tasa_recuperacion_departamento")


# Crear una vista temporal mortalidad_municipios con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW mortalidad_municipios AS
SELECT municipio_nombre,
       COUNT(CASE WHEN fecha_muerte != 'N/A' THEN 1 END) as total_muertes,
       COUNT(*) as total_casos,
       (COUNT(CASE WHEN fecha_muerte != 'N/A' THEN 1 END) / COUNT(*)) * 100 as porcentaje_mortalidad
FROM api_df
GROUP BY municipio_nombre
ORDER BY porcentaje_mortalidad DESC
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM mortalidad_municipios LIMIT 20").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM mortalidad_municipios").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/mortalidad_municipios")


spark.sql("""
CREATE OR REPLACE TEMP VIEW df_con_dias AS
SELECT *,
       DATEDIFF(fecha_diagnostico, fecha_inicio_sintomas) AS dias_sintomas_diagnostico,
       DATEDIFF(fecha_recuperacion, fecha_diagnostico) AS dias_diagnostico_recuperacion
FROM api_df
""")
# Crear una vista temporal tiempos_promedio con SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW tiempos_promedio AS
SELECT 
    AVG(dias_sintomas_diagnostico) AS promedio_dias_sintomas_diagnostico,
    AVG(dias_diagnostico_recuperacion) AS promedio_dias_diagnostico_recuperacion
FROM df_con_dias
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM tiempos_promedio").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM tiempos_promedio").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/tiempos_promedio")

# Crear una vista temporal con el cálculo de días de recuperación
spark.sql("""
CREATE OR REPLACE TEMP VIEW df_con_dias_recuperacion AS
SELECT *,
       DATEDIFF(fecha_recuperacion, fecha_inicio_sintomas) AS dias_recuperacion
FROM api_df
""")
# Crear una vista temporal con las estadísticas de días de recuperación
spark.sql("""
CREATE OR REPLACE TEMP VIEW dias_recuperacion_estadisticas AS
SELECT municipio_nombre,
       AVG(dias_recuperacion) AS promedio_dias_recuperacion,
       PERCENTILE_APPROX(dias_recuperacion, 0.5) AS mediana_dias_recuperacion
FROM df_con_dias_recuperacion
GROUP BY municipio_nombre
ORDER BY promedio_dias_recuperacion
""")
# Ejecutar la consulta para mostrar los resultados
spark.sql("SELECT * FROM dias_recuperacion_estadisticas LIMIT 20").show()
# Guardar los resultados en un archivo CSV
spark.sql("SELECT * FROM dias_recuperacion_estadisticas").coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(trusted_dir + "/estadisticas_recuperacion_municipios")



