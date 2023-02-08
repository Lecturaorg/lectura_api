from fastapi import FastAPI, Response
import json

app = FastAPI()

@app.get("/data")
def data(response: Response):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    with open("database.json") as json_file: data = json.load(json_file)
    return data



# #    if(type is None):
#         return {"Please enter a type: text, author or edition"}
#     options = ['texts',
#                 'authors',
#                 'editions']

 