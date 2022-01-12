import os
from fastapi import FastAPI
import API.functions as f_api
from starlette.responses import FileResponse

app = FastAPI()

@app.get('/')
def read_root():
    return {"Bienvenido":"La API esta en linea"}

@app.get('/query')
def get_query():
    sql_query = 'SELECT TOP(10) * FROM dbo.creCreditos;' # TODO: Esta linea se remmplaza por la entrada
    df = f_api.get_data(sql_query)
    return df

@app.get('/csv')
def get_csv():
    sql_query = 'SELECT TOP(10) * FROM dbo.creCreditos;' # TODO: Esta linea se remmplaza por la entrada
    file_path = os.getcwd() + '/files/data.csv'
    #f_api.get_file(sql_query, file_path)
    return FileResponse(path=file_path, media_type='application/octet-stream', filename='data.csv')