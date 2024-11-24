import requests
import pandas as pd
from django.shortcuts import render
from io import StringIO

# Agregar diccionario de preguntas de negocio
BUSINESS_QUESTIONS = {
    'casos_por_ubicacion': '¿Cuál es la distribución total de casos por departamento y municipio?',
    'casos_por_dia': '¿Cuál es la tendencia temporal de casos reportados por día?',
    'casos_por_edad': '¿Cómo se distribuyen los casos por grupos de edad?',
    'casos_por_sexo': '¿Cuál es la distribución de casos por sexo?',
    'recuperacion_por_sexo': '¿Cuál es el porcentaje de recuperación discriminado por sexo?',
    'tasa_recuperacion_departamento': '¿Cuál es la tasa de recuperación por departamento?',
    'mortalidad_municipios': '¿Cuáles son los municipios con mayor porcentaje de mortalidad?',
    'tiempos_promedio': '¿Cuál es el tiempo promedio entre síntomas, diagnóstico y recuperación?',
    'estadisticas_recuperacion_municipios': '¿Cuál es el promedio y mediana de días de recuperación por municipio?',
    'transmision_vacunacion': '¿Qué relación existe entre la tasa de transmisión y personas vacunadas?',
    'medidas_control': '¿Cómo se relacionan los casos recuperados con las medidas de control implementadas?',
    'edad_ocupacion_UCI': '¿Qué relación existe entre la edad promedio de los pacientes y la ocupación UCI?'
}

def extract_analysis_name(file_path):
    """
    Extrae el nombre del análisis de la ruta completa del archivo
    Ejemplo: 
    'REFINED/bussiness_analytics/dataframes/casos_por_ubicacion/part-00000-XXX.csv' 
    -> 'casos_por_ubicacion'
    """
    parts = file_path.split('/')
    for part in parts:
        if part in BUSINESS_QUESTIONS:
            return part
    return None

def covid_dashboard(request):
    # Consumir el endpoint
    url = "https://023jqmkgqc.execute-api.us-east-1.amazonaws.com/default/showAnalisis"
    
    try:
        # Obtener la respuesta
        response = requests.get(url)
        response.raise_for_status()  # Esto lanzará una excepción si hay error HTTP
        data = response.json()
        
        # Verificar que data es una lista
        if not isinstance(data, list):
            raise ValueError(f"Expected list but got {type(data)}")
            
        # Diccionario para almacenar los dataframes
        dataframes = {}
        
        # Procesar cada archivo
        for file_data in data:
            try:
                if not isinstance(file_data, dict) or 'file_name' not in file_data or 'file_content' not in file_data:
                    continue
                    
                # Extraer el nombre del análisis de la ruta del archivo
                analysis_name = extract_analysis_name(file_data['file_name'])
                
                df = pd.read_csv(StringIO(file_data['file_content']))
                
                # Obtener la pregunta de negocio
                business_question = BUSINESS_QUESTIONS.get(
                    analysis_name,
                    f"Análisis adicional: {file_data['file_name'].split('/')[-1]}"
                )
                
                html_table = df.to_html(
                    classes=['table', 'table-striped', 'table-hover', 'table-responsive'],
                    index=False,
                    escape=False
                )
                
                # Usar el nombre del análisis como clave si está disponible
                key = analysis_name if analysis_name else file_data['file_name'].split('/')[-1].split('.')[0]
                
                dataframes[key] = {
                    'table': html_table,
                    'columns': df.columns.tolist(),
                    'rows': len(df),
                    'business_question': business_question
                }
                
            except Exception as e:
                print(f"Error processing file {file_data.get('file_name', 'unknown')}: {str(e)}")
                continue
        
        return render(request, 'visualization/covid_dashboard.html', {'dataframes': dataframes})
        
    except requests.RequestException as e:
        # Error al hacer la petición HTTP
        error_message = f"Error fetching data: {str(e)}"
        return render(request, 'visualization/covid_dashboard.html', {'error': error_message})
        
    except Exception as e:
        # Cualquier otro error
        error_message = f"Unexpected error: {str(e)}"
        return render(request, 'visualization/covid_dashboard.html', {'error': error_message})