from fastapi import FastAPI, Response, Request
import json
from datetime import datetime
from validation import extract_keys, checkDuplicates, searchDict

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
            if key not in i.keys(): 
                if key == "label":
                    if import_type == "texts":
                        i[key] = i["title"]
                    elif import_type == "authors":
                        i[key] = i["name"]
                else: i[key] = ""
            else: i[key] = i[key]
        new_data.append(i)
    if len(new_data)!=0: new_data = checkDuplicates(new_data,import_data) #Checks if import already exists
    if len(new_data)>0: combined = new_data+import_data
    else: combined = import_data
    with open(file_name, "w") as outfile: json.dump(combined, outfile)
    return {
        "status" : "SUCCESS",
        "data" : req_info
    }

@app.get("/import/approve")
async def importApproval(type, response: Response):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    file_name = type + "_import.json"
    with open(file_name) as json_file: importdata = json.load(json_file)
    with open("database.json") as db_file: db_data = json.load(db_file)
    dataToChange = db_data[type]
    non_duplicates = checkDuplicates(importdata, dataToChange)
    dataToChange = dataToChange+non_duplicates
    db_data[type] = dataToChange
    with open("approved_imports.json") as json_file: approved_imports = json.load(json_file)
    new_approved = {
        'data': importdata,
        'date_approved': datetime.today().strftime('%Y-%m-%d')
    }
    approved_imports[type].append(new_approved)
    with open("approved_imports.json","w") as outfile: json.dump(approved_imports,outfile) 
    importdata = []
    with open("database.json","w") as outfile: json.dump(db_data,outfile)
    with open(file_name,"w") as outfile: json.dump(importdata,outfile)
    return "Imports have been approved"


@app.get("/import_data")
def data(response: Response, type = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    file_name = type+"_import.json"
    with open(file_name) as json_file: data = json.load(json_file)
    return data

@app.get("/search")
def search(response: Response, query, type = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    if type == None:
        with open ("database.json") as json_file: data = json.load(json_file)
        results = {}
        for key in ["authors", "texts","editions"]:
            dataToSearch = data[key]
            results[key] = searchDict(dataToSearch, query)
        return results
