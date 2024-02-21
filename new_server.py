import uvicorn
from fastapi import FastAPI, HTTPException

app = FastAPI()


@app.get("/")
def index():
    return {"success": True, "message": "Hello World!"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
