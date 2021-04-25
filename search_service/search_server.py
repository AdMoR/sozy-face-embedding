import json
from typing import List
from fastapi import FastAPI, HTTPException
from annoy import AnnoyIndex
import numpy as np
from pydantic import BaseModel

app = FastAPI()
u = AnnoyIndex(128, 'angular')
u.load('test.ann') # super fast, will just mmap the file
meta = json.load(open("metadata.json"))


class SearchQuery(BaseModel):
    vector: List[float]
    n: int


@app.post("/search/", status_code=200)
async def vec_seach(search_q: SearchQuery):
    if not len(search_q.vector) == 128:
        raise HTTPException(status_code=400, detail="Vector should have size 128")
    uids = u.get_nns_by_vector(np.array(search_q.vector), search_q.n)
    return {"results": [meta[i] for i in uids]}