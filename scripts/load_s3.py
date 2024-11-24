import mysql.connector
import csv
from io import StringIO
import pandas as pd

# Configuración de la base de datos
db_config = {
    'host': '44.194.118.255', 
    'user': 'my_user',           
    'password': 'my_password',   
    'database': 'covid',    
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
    'key': 'ASIAT2RS5RDCI4Z4WPNU',
    'secret': 'aWsxH2sosn4+Ms/2qkr1HRdac71bbP2dOubCY+Sv',
    'token': 'IQoJb3JpZ2luX2VjEEQaCXVzLXdlc3QtMiJHMEUCIQCdT3m/2bIflSZ25kTBOzJJSpbs2i8xYTUxZdTQl+IvQwIgAplfDY7Lq2zsBebnk1sarVYSBqXrZkFIvLo1hgWKVhYquAII3f//////////ARACGgwyNjMxNzM1NDAwMzYiDD33+Od4dTMNuD3PpSqMAoFHLjwgTkRFZcrVw6DTrr4qYOTBQikBZlCdnsGbojPlk2GoWclgjuRRS4o1IFmHKMQp1l6e2kQAtrmFNBCU0bO8DQe3fpjbeSdwfFfHfbNxj36gfFvnG42mxNvlMWRWxqfggtYu6TWzmWLueBzH3UM1D8EmqFA5h1izAXaV9pek2IF/RcjqIdNGqbmiEk4FmLGQ7e5eUV4TjvVeLCDibuC7pFmU2Gq9AIxZLDeJxLgzRNi/DsgZFuh40esprzAP2No0MQuR+Exd+MGYl6i+P3owc3dFGGUGG+8sUnhPZmseoe3whMvs7tQZMd1JaZrAJgXNXgIleghZLoACnTCzrxGGvnFd88t3J3nO8d4wltmIugY6nQGaZe1SLR9pUvVR1leKES+wbaPjxH8U4JxwtfE5kORWsQiSr9dRBGEo4u7C0V5nzWOGShc5k0Zmo0hqMdHFhvUNbYsSTLvpvOt9en50dAxfILReAEk4PQMty9dCh4Fog3D66uyNY6CXvWeo8/zRA7NqE9Nnz772SqfQf+Q3b98Eo1Qoj3X7tabVAUDO0n85Qkv/Mj8uXDLh02vihvE0'
}

# Guardar el DataFrame como CSV en un servicio externo (como S3)
results_df.to_csv('s3://big-data-topicos/RAW/db_data.csv', storage_options=storage_options, index=False)

# Cerrar la conexión
cursor.close()
conn.close()
