import os
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import API.functions as f_api
from starlette.responses import FileResponse

# Query model
class Query(BaseModel):
    source: str
    sql_query: str
    date: datetime = datetime.now()
    
app = FastAPI()

# Root route
@app.get('/')
def read_root():
    return {"Bienvenido":"La API esta en linea"}

# Get query like json
@app.get('/prueba')
def get_query(query: Query):
    sql_query = 'SELECT TOP(10) * FROM dbo.creCreditos;' # Query de prueba
    df = f_api.get_data(sql_query)
    return df

# Get query like json
@app.get('/query')
def get_query(query: Query):
    df = f_api.get_data(query.sql_query)
    return df

# Get query like csv
@app.get('/csv')
def get_csv(query: Query):
    file_path = os.getcwd() + '/files/data.csv'
    resp = f_api.get_file(query.sql_query, file_path)
    if 'Error' in resp:
        return resp
    else:
        print("Archivo %s" %file_path)
        return FileResponse(path=file_path, media_type='application/octet-stream', filename='data.csv')