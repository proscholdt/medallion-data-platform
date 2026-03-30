from fastapi import FastAPI
from api.pipedrive.pipedrive_router import router as pipedrive_router
from api.facebook.facebook_router import router as facebook_router  # NOVO

app = FastAPI()

# Incluindo rotas
app.include_router(pipedrive_router)
app.include_router(facebook_router)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000)
