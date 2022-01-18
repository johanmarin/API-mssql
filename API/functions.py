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

def run_query(sql_query: str) -> pd.DataFrame:
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
        return df
    
def get_data(sql_query: str) -> dict:
    """Esta función recibe una query SQL en formato string y devuelve la consulta en formato dicionario, eliminando los valores 

    Args:
        sql_query (str): Consulta sql en formato string

    Returns:
        dict: diccionario de lso datos
    """    
    df = run_query(sql_query) 
    
    if type(df) == dict:
        return df

    else:
        # Convirtiendo fechas en datos legibles
        for col in df.columns:
            if type(df[col][0]) in [pd._libs.tslibs.timestamps.Timestamp, pd._libs.tslibs.nattype.NaTType]:
                df[col] = df[col].apply(lambda x: x.isoformat())
        return json.loads(simplejson.dumps(df.to_dict(), ignore_nan=True))

def get_file(sql_query: str, file_path: str):
    """Esta fucnión obtiene los datos de una consulta y los pone en un archivo csv

    Args:
        sql_query (str): query que permite obteern los datos desde la base de datos
        file_path (str): ruta del archivo donde se guardan los datos como csv
    """    
    
    if os.path.exists(file_path):
        os.remove(file_path)

    df = run_query(sql_query) # Extrallendo los datos
    
    if type(df) == dict:
        return df
    else:
        df.to_csv(file_path, index=False)
        return {'Confirmación':'Se han extraido los datos correctamente'}