import os
from flask import Flask, jsonify, send_from_directory
import API.functions as f_api


app = Flask(__name__)

app.config["CLIENT_CSV"] = os.getcwd() + '/files/'

# Testing Route
@app.route('/', methods=['GET'])
def test():  
    return jsonify({"Bienvenido":"La API esta en linea"})

@app.route('/query', methods=['GET'])
def get_query():
    df = f_api.get_data('SELECT TOP(10) * FROM dbo.creCreditos;')
    return jsonify(df)

@app.route('/csv', methods=['GET'])
def get_csv():
    sql_query = 'SELECT TOP(10) * FROM dbo.creCreditos;' # TODO: Esta linea se remmplaza por la entrada
    path = app.config["CLIENT_CSV"] + 'data.csv'
    resp = f_api.get_file(sql_query, path)
    if 'Error' in resp:
        return jsonify(resp)
    else:
        print("Archivo %s" %path)
        return send_from_directory(app.config["CLIENT_CSV"], path, 'data.csv', as_attachment = True)
    

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
    