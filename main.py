import os
import yaml
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import API.functions as f_api
from starlette.responses import FileResponse

# Load config
def load_config():    
    f = open(os.getcwd().replace('\\', '/') + '/config.yaml' )
    config = yaml.load(f, Loader=yaml.FullLoader)
    f.close()
    return config

# Conexion
config = load_config()
visual_conn = f_api.connect(config)

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
def get_query_prueba():
    df = f_api.get_data(visual_conn, config['query_prueba'], config)
    return df

# Get query like json
@app.get('/query')
def get_query(query: Query):
    df = f_api.get_data(visual_conn, query.sql_query, config)
    print(df)
    return df

# Get query like csv
@app.get('/csv')
def get_csv(query: Query):
    file_path = os.getcwd() + '/files/data.csv'
    resp = f_api.get_file(visual_conn, query.sql_query, config, file_path)
    if 'Error' in resp:
        return resp
    else:
        print("Archivo %s" %file_path)
        return FileResponse(path=file_path, media_type='application/octet-stream', filename='data.csv')