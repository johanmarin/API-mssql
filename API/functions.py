import os
import yaml
import json
import pandas as pd
import simplejson
from db_conexion.conexion import VisualQuery

def get_config() -> dict:
    """Lee la configuración para conectarse al servidor desde el archivo config.json  y la devulve como diccionario

    Returns:
        dict: Configuración para conectarse al servido de bases de datos de 
    """    
    path = '/config.yaml'  
    f = open(os.getcwd().replace('\\', '/')+path)
    config = yaml.load(f, Loader=yaml.FullLoader)
    f.close()
    return config

def get_data(sql_query: str) -> pd.DataFrame:
    """Esta función recibe una query SQL en formato string y devuelve la consulta en formato dataframe

    Args:
        sql_query (str): Consulta sql en formato string

    Returns:
        pd.DataFrame: Salida de la quer
    """    
    config = get_config()
    print(config)
    driver = config['driver']
    server = config['server']
    database = config['database']
    username = config['username']
    password = config['password']
        
    print('Ejecutando consulta')
    qry = VisualQuery(driver, server, database, username, password, sql_query)
    print('Conectando a la base de datos')
    qry.connect()
    if qry.cnxn == False:
        return {'Error':'No ha sido posible conectarse a la base de datos'}
    else:
        print('Extrayendo datos de la base de datos')
        qry.run_query()
        print('Guardando los datos en un dataframes')
        df = qry.get_data()
        qry.close_conexion()
        print('Cerrando conexión')    
        js = simplejson.dumps(df.to_dict(), ignore_nan=True)
        return json.loads(js)