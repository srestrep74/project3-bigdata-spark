import pandas as pd
from sodapy import Socrata
import os
MyAppToken = "#"

client = Socrata("www.datos.gov.co",
                 MyAppToken,
                 username="#",
                 password="#")

results = client.get("gt2j-8ykr", limit=1000)
results_df = pd.DataFrame.from_records(results)

storage_options = {
    'key': os.getenv('AWS_ACCESS_KEY_ID'),  
    'secret': os.getenv('AWS_SECRET_ACCESS_KEY'),  
    'token': os.getenv('AWS_SESSION_TOKEN') 
}

results_df.to_csv('s3://big-data-topicos/RAW/covid_data.csv', storage_options=storage_options, index=False)