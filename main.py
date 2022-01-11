from fastapi import FastAPI
import API.functions as f_api

import pandas as pd
import simplejson
import json

app = FastAPI()

@app.get('/')
def read_root():
    return {"Bienvenido":"La API esta en linea"}

@app.get('/prueba')
def get_query():
    df = json.loads(simplejson.dumps(pd.read_csv('./expl.csv').to_dict(), ignore_nan=True))
    return df

@app.get('/query')
def get_query():
    df = f_api.get_data('SELECT TOP(10) * FROM dbo.creCreditos;')
    return df