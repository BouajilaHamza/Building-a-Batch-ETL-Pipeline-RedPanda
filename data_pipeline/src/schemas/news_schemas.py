from typing import List

from pydantic import BaseModel, RootModel


class NewsDoc(BaseModel):
    description: str
    source: str
    pubDate: str
    title: str


class NewsData(RootModel):
    root: List[NewsDoc]
