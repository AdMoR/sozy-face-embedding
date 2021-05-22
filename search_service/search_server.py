import json
from typing import List
from fastapi import FastAPI, HTTPException, Request, File, UploadFile
from fastapi.responses import HTMLResponse
from annoy import AnnoyIndex
import numpy as np
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import face_recognition
import base64


app = FastAPI()
u = AnnoyIndex(128, 'angular')
u.load('../test.ann') # super fast, will just mmap the file
meta = json.load(open("../metadata.json"))


app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="template")



class SearchQuery(BaseModel):
    vector: List[float]
    n: int

class WebcamQuery(BaseModel):
    filename: str


@app.post("/search_vector/", status_code=200)
async def vec_seach(search_q: SearchQuery):
    if not len(search_q.vector) == 128:
        raise HTTPException(status_code=400, detail="Vector should have size 128")
    uids = u.get_nns_by_vector(np.array(search_q.vector), search_q.n)
    return {"results": [meta[i] for i in uids]}


@app.post("/search/", status_code=200)
async def face_search(request: Request, image: UploadFile = File(...)):
    face_embeddings = list()
    image = face_recognition.load_image_file(image.file)
    # top, right, bottom and left coordinates for face locations
    face_locations = face_recognition.api.face_locations(image, model="cnn")

    if len(face_locations) == 0:
        raise HTTPException(status_code=400, detail="No face found")

    def min_size(face_loc):
        return (face_loc[1] - face_loc[3]) * (face_loc[2] - face_loc[0]) > 60 ** 2
    face_locations = list(filter(min_size, face_locations))

    if len(face_locations) == 0:
        raise HTTPException(status_code=400, detail="Face too small")

    face_embeddings += face_recognition.face_encodings(image, known_face_locations=face_locations, model="large")

    uids = u.get_nns_by_vector(np.array(face_embeddings[0]), 10)
    returned_elements = list(map(lambda x: x.split(";"), [meta[i] for i in uids]))
    urls, bbs = list(zip(*returned_elements))
    bbs = list(map(eval, bbs))
    return templates.TemplateResponse("home.html",
                                      {"request": request,
                                       "corresponding_matches": list(zip(urls, bbs))})


@app.get("/", response_class=HTMLResponse)
async def read_item(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/webcam", response_class=HTMLResponse)
async def read_item(request: Request):
    return templates.TemplateResponse("webcam.html", {"request": request, "SERVER_URL": request.url_for("face_search")})
