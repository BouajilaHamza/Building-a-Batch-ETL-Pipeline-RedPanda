from datetime import datetime
from typing import List
from uuid import uuid4

from pydantic import BaseModel, RootModel


class BitcoinDoc(BaseModel):
    id: str = uuid4().hex
    price: float
    volume_24h: float
    volume_change_24h: float
    percent_change_1h: float
    percent_change_24h: float
    percent_change_7d: float
    market_cap: float
    market_cap_dominance: float
    fully_diluted_market_cap: float
    last_updated: datetime


class BitcoinData(RootModel):
    root: List[BitcoinDoc]
