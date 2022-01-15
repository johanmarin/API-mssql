import os
import json
import API.functions as f_api
from pydantic import BaseModel
from flask import Flask, jsonify, send_from_directory, request

app = Flask(__name__)

app.config["CLIENT_CSV"] = os.getcwd() + '/files/'

# Testing Route
@app.route('/', methods=['GET'])
def test():  
    return jsonify({"Bienvenido":"La API esta en linea"})

@app.route('/prueba', methods=['GET'])
def get_prueba():
    df = f_api.get_data('SELECT TOP(10) * FROM dbo.creCreditos;')
    return jsonify(df)

@app.route('/query', methods=['GET'])
def get_query():
    data = json.loads(request.data)
    df = f_api.get_data(data['sql_query'])
    return jsonify(df)

@app.route('/csv', methods=['GET'])
def get_csv():
    data = json.loads(request.data)
    path = app.config["CLIENT_CSV"] + 'data.csv'
    resp = f_api.get_file(data['sql_query'], path)
    if 'Error' in resp:
        return jsonify(resp)
    else:
        print("Archivo %s" %path)
        return send_from_directory(app.config["CLIENT_CSV"], path, 'data.csv', as_attachment = True)
    

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
    