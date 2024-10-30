from fastapi import FastAPI
from app.routes import router

app = FastAPI()

app.include_router(router)

app.title = "API de JosueCompany"

@app.get("/")
async def root():
    return {"message": "Bienvenido a la API de JosueCompany"}
