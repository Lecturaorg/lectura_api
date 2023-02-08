from fastapi import FastAPI, Response, Request
from pydantic import BaseModel
import json
import sys
from pympler.asizeof import asizeof

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


class Item(BaseModel):
    type: str
    query: str

app = FastAPI()

@app.get("/data")
def data(response: Response):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    with open("database.json") as json_file: data = json.load(json_file)
    return data

@app.post("/import")
async def import_data(info: Request, response: Response):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    req_info = await info.json()
    with open("imported_data/"+req_info["name"]+".json", "w") as outfile:
        json.dump(req_info, outfile)
    return {
        "status" : "SUCCESS",
        "data" : req_info
    }

#@app.get("/search")
#def searchData(item:Item):
 #   item.query
# #    if(type is None):
#         return {"Please enter a type: text, author or edition"}
#     options = ['texts',
#                 'authors',
#                 'editions']

 