# import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()

# Load covid api database csv
covid_api_df = spark.read.csv('s3://big-data-topicos/RAW/covid_data.csv',inferSchema=True,header=True)

# load covid local database csv
covid_local_df = spark.read.csv('s3://big-data-topicos/RAW/db_data.csv',inferSchema=True,header=True)

# Reconocimiento del dataframe
def print_df_columns(df, df_name):
    print(f'{df_name} dataframe columns:')
    for column in df.columns:
        print(f' - {column}')
    print()
    
print_df_columns(covid_local_df, 'Covid local')
print_df_columns(covid_api_df, 'Covid API')

# Delete unnecessary columns

columns_to_drop = [
    'id_de_caso',
    'fecha_de_notificaci_n',
    'unidad_medida',
    'fuente_tipo_contagio',
    'ubicacion', 'estado',
    'tipo_recuperacion',
    'per_etn_',
    'nom_grupo_'
]

covid_api_df = covid_api_df.drop(*columns_to_drop)

print_df_columns(covid_api_df, 'Covid API')

# Delete unnecessary columns

columns_to_drop = [
    'casos_positivos'
]

covid_local_df = covid_local_df.drop(*columns_to_drop)

print_df_columns(covid_local_df, 'Covid API')

# Normalize columns to follow the standard

import unicodedata

def normalize_column_names(df, df_name):
    normalized_columns = []
    for column_name in df.columns:
        # Replace spaces with underscores
        column_name = column_name.replace(" ", "_")

        # Remove leading and trailing spaces
        column_name = column_name.strip()

        # Convert to lowercase
        column_name = column_name.lower()

        # Remove accents and special characters
        column_name = ''.join(
            c for c in unicodedata.normalize('NFD', column_name) if unicodedata.category(c) != 'Mn'
        )

        normalized_columns.append(column_name)

    df = df.toDF(*normalized_columns)
    
    print_df_columns(df, df_name)


normalize_column_names(covid_local_df, 'Covid local')
normalize_column_names(covid_api_df, 'Covid API')

# Rename columns to follow business logic
new_column_names = {
    'fecha': 'fecha_reporte',
    'fecha_reporte_web': 'fecha_reporte',
    'departamento': 'departamento_id',
    'departamento_nom': 'departamento_nombre',
    'ciudad_municipio': 'municipio_id',
    'ciudad_municipio_nom': 'municipio_nombre',
    'fecha_recuperado': 'fecha_recuperacion'
}

def rename_columns(df, df_name):
    for column_name in df.columns:
        if column_name in new_column_names:
            df = df.withColumnRenamed(column_name, new_column_names[column_name])

    print_df_columns(df, df_name)
    return df

covid_local_df = rename_columns(covid_local_df, 'Covid local')
covid_api_df = rename_columns(covid_api_df, 'Covid API')

# Delete duplicate data

print(f'Covid local rows before drop duplicates: {covid_local_df.count()}')
covid_local_df = covid_local_df.dropDuplicates()
print(f'Covid local rows after drop duplicates: {covid_local_df.count()}')

print()

print(f'Covid API rows before drop duplicates: {covid_api_df.count()}')
covid_api_df = covid_api_df.dropDuplicates()
print(f'Covid API rows after drop duplicates: {covid_api_df.count()}')

# Standardize null values

from pyspark.sql.functions import when, col

def clean_blank_data(df, df_name):
    for column_name in df.columns:
        # Count rows with NULL values before cleaning
        null_count_before = df.filter(col(column_name).isNull()).count()
        
        # Replace NULL values with 'N/A' for each column
        df = df.withColumn(column_name, when(col(column_name).isNull(), "N/A").otherwise(col(column_name)))
        
        # Count rows with NULL values after cleaning
        null_count_after = df.filter(col(column_name).isNull()).count()
        
        print(f"{df_name} - {null_count_before} rows modified")
    
    print()

    return df

covid_local_df = clean_blank_data(covid_local_df, "Covid local")
covid_api_df = clean_blank_data(covid_api_df, "Covid API")

# Format date columns

from pyspark.sql.functions import col, split, trim, to_date

def convert_date_columns(df):
    for column_name in df.columns:
        if "fecha" in column_name.lower():  # Checks if the column name contains the word 'date'
            # Remove the time part, keeping only the date
            df = df.withColumn(column_name, trim(split(col(column_name), ' ').getItem(0)))
    
    return df


covid_local_df = convert_date_columns(covid_local_df)
covid_api_df = convert_date_columns(covid_api_df)

covid_local_df.show(5)
covid_api_df.show(5)

import pandas as pd

# Convertir a Pandas y guardar con nombre específico
covid_local_df.toPandas().to_csv("s3://big-data-topicos/TRUSTED/local_db.csv", index=False)
covid_api_df.toPandas().to_csv("s3://big-data-topicos/TRUSTED/api_db.csv", index=False)
