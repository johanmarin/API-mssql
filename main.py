from fastapi import FastAPI

app = FastAPI()

@app.get('/')
def read_root():
    return {"welcome":"La API esta en linea"}