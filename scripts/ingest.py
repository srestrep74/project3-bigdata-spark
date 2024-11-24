import pandas as pd
from sodapy import Socrata


MyAppToken = "yt3Pah6A6ldZWtxW0Nzp9lfbb"


client = Socrata("www.datos.gov.co",
                  MyAppToken,
                  username="jmgomezp@eafit.edu.co",
                  password="EAFIT2024...**")


results = client.get("gt2j-8ykr", limit=1000)
results_df = pd.DataFrame.from_records(results)


storage_options = {
    'key': 'ASIAT2RS5RDCI4Z4WPNU',
    'secret': 'aWsxH2sosn4+Ms/2qkr1HRdac71bbP2dOubCY+Sv',
    'token': 'IQoJb3JpZ2luX2VjEEQaCXVzLXdlc3QtMiJHMEUCIQCdT3m/2bIflSZ25kTBOzJJSpbs2i8xYTUxZdTQl+IvQwIgAplfDY7Lq2zsBebnk1sarVYSBqXrZkFIvLo1hgWKVhYquAII3f//////////ARACGgwyNjMxNzM1NDAwMzYiDD33+Od4dTMNuD3PpSqMAoFHLjwgTkRFZcrVw6DTrr4qYOTBQikBZlCdnsGbojPlk2GoWclgjuRRS4o1IFmHKMQp1l6e2kQAtrmFNBCU0bO8DQe3fpjbeSdwfFfHfbNxj36gfFvnG42mxNvlMWRWxqfggtYu6TWzmWLueBzH3UM1D8EmqFA5h1izAXaV9pek2IF/RcjqIdNGqbmiEk4FmLGQ7e5eUV4TjvVeLCDibuC7pFmU2Gq9AIxZLDeJxLgzRNi/DsgZFuh40esprzAP2No0MQuR+Exd+MGYl6i+P3owc3dFGGUGG+8sUnhPZmseoe3whMvs7tQZMd1JaZrAJgXNXgIleghZLoACnTCzrxGGvnFd88t3J3nO8d4wltmIugY6nQGaZe1SLR9pUvVR1leKES+wbaPjxH8U4JxwtfE5kORWsQiSr9dRBGEo4u7C0V5nzWOGShc5k0Zmo0hqMdHFhvUNbYsSTLvpvOt9en50dAxfILReAEk4PQMty9dCh4Fog3D66uyNY6CXvWeo8/zRA7NqE9Nnz772SqfQf+Q3b98Eo1Qoj3X7tabVAUDO0n85Qkv/Mj8uXDLh02vihvE0'
}


results_df.to_csv('s3://big-data-topicos/RAW/covid_data.csv', storage_options=storage_options, index=False)