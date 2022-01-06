from fastapi import FastAPI

app = fastAPI()

@app.get('/')
def read_root():
    return {"welcome":"La API esta en linea"}