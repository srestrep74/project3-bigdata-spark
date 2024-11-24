import mysql.connector
import csv
from io import StringIO
import pandas as pd

# Configuración de la base de datos
db_config = {
    'host': '#', 
    'user': '#',           
    'password': '#',   
    'database': '#',    
}

# Conectar a la base de datos
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Ejecutar la consulta
query = """
SELECT fecha, casos_positivos, pruebas_realizadas, 
       personas_vacunadas, ocupacion_uci, 
       tasa_transmision, medidas_control 
FROM covid_data
"""
cursor.execute(query)

# Obtener los resultados
rows = cursor.fetchall()

# Crear un archivo CSV en memoria
csv_file = StringIO()
csv_writer = csv.writer(csv_file)

# Escribir encabezado
csv_writer.writerow(['fecha', 'casos_positivos', 'pruebas_realizadas', 
                     'personas_vacunadas', 'ocupacion_uci', 
                     'tasa_transmision', 'medidas_control'])

# Escribir los datos
csv_writer.writerows(rows)

# Posicionar el cursor al inicio del archivo CSV en memoria
csv_file.seek(0)

# Convertir el CSV a un DataFrame de Pandas
results_df = pd.read_csv(csv_file)

# Mostrar el DataFrame
print(results_df)

# Opcional: Guardar a S3 si es necesario
storage_options = {
    'key': '#',
    'secret': '#',
    'token': '#'
}

# Guardar el DataFrame como CSV en un servicio externo (como S3)
results_df.to_csv('s3://big-data-topicos/RAW/db_data.csv', storage_options=storage_options, index=False)

# Cerrar la conexión
cursor.close()
conn.close()
