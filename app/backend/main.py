# backend/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from storage.motherduck import BitcoinDataLoader as MotherDuckBitcoinLoader
from storage.motherduck import NewsDataLoader as MotherDuckNewsLoader
from storage.snowflake import BitcoinDataLoader as SnowflakeBitcoinLoader
from storage.snowflake import NewsDataLoader as SnowflakeNewsLoader

app = FastAPI()


class LoadRequest(BaseModel):
    loader_type: str
    data_type: str
    data: dict


@app.post("/load-data/")
async def load_data(request: LoadRequest):
    loader_type = request.loader_type
    data_type = request.data_type
    raw_data = request.data

    if loader_type == "snowflake":
        if data_type == "news":
            loader = SnowflakeNewsLoader()
        elif data_type == "bitcoin":
            loader = SnowflakeBitcoinLoader()
        else:
            raise HTTPException(status_code=400, detail="Invalid data type")
    elif loader_type == "motherduck":
        if data_type == "news":
            loader = MotherDuckNewsLoader()
        elif data_type == "bitcoin":
            loader = MotherDuckBitcoinLoader()
        else:
            raise HTTPException(status_code=400, detail="Invalid data type")
    else:
        raise HTTPException(status_code=400, detail="Invalid loader type")

    try:
        loader.load_data(raw_data)
        loader.close_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {e}")

    return {"message": "Data loaded successfully"}
