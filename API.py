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
    non_duplicates = []#data
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
                    oldValue = j[key]
                    newValue = i[key]
                    if isinstance(oldValue,float): oldValue = int(oldValue)
                    if isinstance(newValue,float): newValue = int(newValue)
                    oldValue = str(oldValue).split(", ")
                    newValue = str(newValue).split(", ")
                    if len(oldValue) == len(newValue):
                        oldValue = ", ".join(oldValue)
                        newValue = ", ".join(newValue)
                    else:
                        stringCheckNew = stringCheckNew + newValue[0]
                        stringCheckOld = stringCheckOld + oldValue[0]
            if stringCheckNew == stringCheckOld: duplicate = True
        if duplicate == False: non_duplicates.append(i)
    return non_duplicates

def searchDict(data, query):
    querySplit = query.split(" ")
    for element in querySplit:
        results = []
        for i in data:
            found = False
            if found == False:
                for key in i.keys():
                    if str(element).lower() in str(i[key]).lower():
                        if found == False: results.append(i)
                        found = True
        data = results
    return results

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
    database = open("database.json")
    database_load = json.load(database)[import_type]
    database.close()
    data = checkDuplicates(data,database_load) ##Checks database if it exists
    new_data = []
    for i in data: ##Adds columns/keys that exists in the database -> formatting needed
        i["date_uploaded"] = date_uploaded
        keysToCheck = cols[import_type]
        for key in keysToCheck:
           if key not in i.keys(): i[key] = ""
           else: i[key] = i[key]
        new_data.append(i)
    new_data = checkDuplicates(new_data,import_data) #Checks if import already exists
    if len(new_data)>0: combined = new_data+import_data
    else: combined = import_data
    with open(file_name, "w") as outfile: json.dump(combined, outfile)
    return {
        "status" : "SUCCESS",
        "data" : req_info
    }

@app.get("/import_data")
def data(response: Response, type = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    file_name = type+"_import.json"
    with open(file_name) as json_file: data = json.load(json_file)
    return data

@app.get("/search")
def search(response: Response, query, type = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    #req_info = await info.json()
    #if "filters" in req_info.keys():
    #    return ("filters found")
    if type == None:
        with open ("database.json") as json_file: data = json.load(json_file)
        results = {}
        for key in ["authors", "texts","editions"]:
            dataToSearch = data[key]
            results[key] = searchDict(dataToSearch, query)
        return results


#@app.get("/search")
#def searchData(item:Item):
 #   item.query
# #    if(type is None):
#         return {"Please enter a type: text, author or edition"}
#     options = ['texts',
#                 'authors',
#                 'editions']

 