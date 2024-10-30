from fastapi import FastAPI
from app.routes import router

app = FastAPI()

app.include_router(router)

app.title = "API de Josue"

@app.get("/")
async def root():
    return {"message": "Bienvenido a la API de Josue"}
