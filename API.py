from fastapi import FastAPI, Response, Request
from pydantic import BaseModel
import json
import sys
#from pympler.asizeof import asizeof

##Get keys of each type -> use keys to find duplicate
def extract_keys():
    file = open("database.json")
    data = json.load(file)
    cols = {
        'texts':list(data["texts"][0].keys()),
        'authors': list(data["authors"][0].keys()),
        'editions': list(data["editions"][0].keys()),
    }
    file.close()
    return cols

def checkDuplicates(newData, data):
    non_duplicates = data
    for i in newData:
        keysToCheck = i.keys()
        duplicate = False
        for j in data:
            stringCheckNew = ""
            stringCheckOld = ""
            for key in keysToCheck: ##Check every key & value
                if key not in j.keys() or key == "date_uploaded":
                    continue
                else:
                    dataNew = str(i[key]).split(", ")
                    dataOld = str(j[key]).split(", ")
                    for element in dataNew:
                        for oldElement in dataOld:
                            if element == oldElement and element != "":
                                continue
                            else: 
                                stringCheckNew = stringCheckNew + element
                                stringCheckOld = stringCheckOld + oldElement
            if stringCheckNew == stringCheckOld: duplicate = True                            
        if duplicate == False: non_duplicates.append(i)
    return non_duplicates

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
    import_type = req_info["type"]
    file_name = import_type+'_import.json'
    json_file = open(file_name)
    import_data = json.load(json_file)
    json_file.close()
    date_uploaded = req_info["date_uploaded"]
    data = req_info["data"]
    cols = extract_keys()
    new_data = []
    for i in data: 
        i["date_uploaded"] = date_uploaded
        keysToCheck = cols[import_type]
        for key in keysToCheck:
           if key not in i.keys(): i[key] = ""
        new_data.append(i)
    new_data = checkDuplicates(new_data,import_data)
    # print(data)
    with open(file_name, "w") as outfile: json.dump(new_data, outfile)
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

 