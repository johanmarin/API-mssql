import os
import json
import pyodbc
import pandas as pd
import simplejson
from db_conexion.conexion import VisualQuery

def connect(config: dict):
    """Esta funcion crea una instancia de conexión con la base de datos y la conecta

    Args:
        config (dict): datos para realizar la conexión

    Returns:
        [type]: instancia conectada
    """    
    
    driver = config['driver']
    server = config['server']
    database = config['database']
    port = str(config['port'])
    username = config['username']
    password = config['password']
    sql_query = config['query_prueba']
    visual_conn = VisualQuery(driver, server, database, port, username, password, sql_query)
    visual_conn.connect()    
    return visual_conn
    

def reconnect(visual_conn: VisualQuery, config: dict):
    """Este programa vuelva a conectar la instancia a la base de datos

    Args:
        visual_conn (VisualQuery): instancia
        config (dict): datos de conexión

    Returns:
        [type]: instancia conectada
    """    
    try: 
        visual_conn.connect()
    except:
        visual_conn = connect(config)
    return visual_conn   

def get_data_query(visual_conn: VisualQuery, sql_query: str) -> pd.DataFrame:
    """REcibe una instancia conectada a la base de datos y una consulta sql y devuelve los datos como dataframe

    Args:
        visual_conn (VisualQuery): instancia conectada
        sql_query (str): query

    Returns:
        pd.DataFrame: datos que devuelve la consulta
    """    
    visual_conn.sql_query = sql_query
    print('Extrayendo datos de la base de datos')
    visual_conn.run_query()
    print('Guardando los datos en un dataframes')
    df = visual_conn.get_data()
    return df 
    

def run_query(visual_conn: VisualQuery, sql_query: str, config: dict) -> pd.DataFrame:
    """Esta función recibe una query SQL en formato string y devuelve la consulta en formato dataframe

    Args:
        sql_query (str): Consulta sql en formato string

    Returns:
        pd.DataFrame: Salida de la quer
    """    
    try:
        df = get_data_query(visual_conn, sql_query)
    except e:
        if e.__class__ == pyodbc.ProgrammingError:        
            visual_conn = reconnect(visual_conn, config)
            
            if visual_conn.cnxn == False:                
                return {'Error':'No ha sido posible conectarse a la base de datos'}
            else:
                df = get_data_query(visual_conn, sql_query)
    return df
     
        
    
def get_data(visual_conn: VisualQuery, sql_query: str, config: dict) -> dict:
    """Esta función recibe una query SQL en formato string y devuelve la consulta en formato dicionario, eliminando los valores 

    Args:
        sql_query (str): Consulta sql en formato string

    Returns:
        dict: diccionario de lso datos
    """    
    df = run_query(visual_conn, sql_query, config) 
    
    if type(df) == dict:
        return df

    else:
        # Convirtiendo fechas en datos legibles
        for col in df.columns:
            if type(df[col][0]) in [pd._libs.tslibs.timestamps.Timestamp, pd._libs.tslibs.nattype.NaTType]:
                df[col] = df[col].apply(lambda x: x.isoformat())
        return json.loads(simplejson.dumps(df.to_dict(), ignore_nan=True))

def get_file(visual_conn: VisualQuery, sql_query: str, config: dict, file_path: str):
    """Esta fucnión obtiene los datos de una consulta y los pone en un archivo csv

    Args:
        sql_query (str): query que permite obteern los datos desde la base de datos
        file_path (str): ruta del archivo donde se guardan los datos como csv
    """    
    
    if os.path.exists(file_path):
        os.remove(file_path)

    df = run_query(visual_conn, sql_query, config) # Extrallendo los datos
    
    if type(df) == dict:
        return df
    else:
        df.to_csv(file_path, index=False)
        return {'Confirmación':'Se han extraido los datos correctamente'}