import pyodbc
import pandas as pd

# Connect to database and return query
class VisualQuery:
    """
    Esta clase permite realizar una query desde la base de datos de Visal Smic

    Args:
        server (str): Direccion IP del servidor
        database (str): Nombre de la base de datos
        port: puerto de conexión con la base de datos
        username (str): Nombre de usuario
        password (str): Contraseña para la base de datos
        query (str): Código para realizar la consulta
        
    Atrib:
        query (str): Es el codigo de la query  
        data (pd.DataFrame): Información de que trae la query despues de ser corrida. Por defecto un DataFrame vacio
    
    Methods:
        get_query: Devuelve el codigo de la query
        connect: Realiza la conexión con la base de datos
        run_query: Ejecuta la query y almacena los datos en el atributo data en formato DataFrame
        get_data: Devuelve lso datos contenidos en el atributo data
    
    """ 
    def __init__(self, driver: str, server: str, database: str, port: str,username:str, password: str, sql_query: str):
        
        
        self.driver = driver
        self._server = server
        self._database = database
        self.port = port
        self._username = username
        self._password = password
        self.sql_query = sql_query
        self.string_conexion = 'DRIVER={'+driver+'};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+password
        self.data = pd.DataFrame()
        
    def get_query(self):
        return self.sql_query
        
    def connect(self):
        
        try:
            print('String de conexión: %s' %self.string_conexion)
            self.cnxn =  pyodbc.connect(self.string_conexion, autocommit=True)
            
            print('conexion exitosa con %s' % self._database)     
        except:
            self.cnxn = False
            print('No ha sido posible conectarse con %s' % self._database)
    
    def update_table(self):
        self.cursor = self.cnxn.cursor()
        self.cursor.execute(self.sql_query)
        self.cnxn.commit()
     
    def run_query(self):
        self.data =  pd.read_sql(self.sql_query, con=self.cnxn)
    
    def get_data(self):
        return self.data  
    
    def close_conexion(self):
        csr = self.cnxn.cursor()  
        csr.close()
        self.cnxn.close() 
        print('Conexion cerrada con %s' % self._database)