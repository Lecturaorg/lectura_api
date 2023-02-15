from fastapi import FastAPI, Response, Request
import json
from datetime import datetime
from validation import extract_keys, checkDuplicates, searchDict
from table_models import engine
import pandas as pd
import numpy as np

app = FastAPI()

@app.get("/data")
def data(response: Response, type = None, id = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    if type != None and id != None:
        if type == 'texts':
            query = '''
                select text_id
                ,text_title || 
                    CASE
                        WHEN TEXT_ORIGINAL_publication_year is null then ''
                        when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC)'
                        else ' (' || text_original_publication_year || ' AD)'
                    end
                label
            from texts
            where author_id =
            ''' + id
            texts = pd.read_sql(query, con=engine()).replace(np.nan, None).to_dict('records')#.to_json(orient="table")
            return texts
    with open("database_test.json") as json_file: data = json.load(json_file)
    return data

@app.post("/edit")
async def edit_data(info: Request, response: Response, type, id):
    response.headers["Access-Control-Allow-Origin"] = "*"
    req_info = await info.json()
    json_file = open("database.json")
    db_data = json.load(json_file)
    json_file.close()
    db_type = db_data[type]
    row = 0
    for x in db_type:
        if x["id"] == int(id): break
        else: row+=1
    db_data[type][row] = req_info
    #with open("database.json", "w") as output_file: json.dump(db_data,output_file)
    return {
        "status" : "SUCCESS",
        "data" : req_info
    }


@app.post("/import")
async def import_data(info: Request, response: Response):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    req_info = await info.json()
    importType = req_info["type"]
    file_name = importType+'_import.json'
    jsonFile = open(file_name)
    import_data = json.load(jsonFile)
    jsonFile.close()
    date_uploaded = req_info["date_uploaded"]
    data = req_info["data"]
    keysToAddIfNotExists = extract_keys()[importType]
    database = open("database.json")
    databaseLoad = json.load(database)[importType]
    database.close()
    maxIndex = max(databaseLoad, key=lambda x:x['id'])["id"]+1
    keys = {'texts': ["title", "author"], "authors": ["name", "birth", "death"], "editions":["title", "author"]} ##Keys to check
    keysToCheck = keys[importType]
    data = checkDuplicates(data,databaseLoad, keysToCheck = keysToCheck, index = maxIndex, importType = importType) ##Checks database if it exists
    new_data = []
    for i in data: ##Adds columns/keys that exists in the database -> formatting needed
        i["date_uploaded"] = date_uploaded
        for key in keysToAddIfNotExists:
            if key not in i.keys(): 
                if key == "label":
                    if importType == "texts":
                        i[key] = i["title"]
                    elif importType == "authors":
                        i[key] = i["name"]
                else: i[key] = ""
            else: i[key] = i[key]
        new_data.append(i)
    if len(new_data)!=0: new_data = checkDuplicates(new_data,import_data, keysToCheck = keysToCheck) #Checks if import already exists
    if len(new_data)>0: combined = new_data+import_data
    else: combined = import_data
    #with open(file_name, "w") as outfile: json.dump(combined, outfile)
    return {
        "status" : "SUCCESS",
        "data" : req_info
    }

@app.post("/import/approve")
async def importApproval(type, response: Response, info: Request):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    approved_data = await info.json()
    file_name = type + "_import.json"
    with open("database.json") as db_file: db_data = json.load(db_file)
    dataToChange = db_data[type]
    keys = {'texts': ["title", "author"], "authors": ["name", "birth", "death"], "editions":["title", "author"]} ##Keys to check
    keysToCheck = keys[type]
    non_duplicates = checkDuplicates(approved_data, dataToChange, keysToCheck = keysToCheck)
    print(non_duplicates)
    nonSimilar = []
    for i in non_duplicates:
        if type == "authors": duplicateCheck = searchDict(dataToChange,i["name"])
        elif type == "texts": duplicateCheck = searchDict(dataToChange,i["title"])
        else: duplicateCheck = searchDict(dataToChange,i["title"])
        if len(duplicateCheck) == 1:
            continue
        elif len(duplicateCheck) == 0:
            nonSimilar.append(i)
    dataToChange = dataToChange+nonSimilar
    db_data[type] = dataToChange
    with open("approved_imports.json") as json_file: approved_imports = json.load(json_file)
    newApproved = {
        'data': nonSimilar,
        'date_approved': datetime.today().strftime('%Y-%m-%d')
    }
    approved_imports[type].append(newApproved)
    #with open("approved_imports.json","w") as outfile: json.dump(approved_imports,outfile) 
    importdata = []
    #with open("database.json","w") as outfile: json.dump(db_data,outfile)
    #with open(file_name,"w") as outfile: json.dump(importdata,outfile)
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
    with open ("database_test.json") as json_file: data = json.load(json_file)
    results = {}
    if type == None: keysToCheck = ["authors", "texts", "editions"]
    else: keysToCheck = [type]
    for key in keysToCheck:
        dataToSearch = data[key]
        result = searchDict(dataToSearch, query)
        if len(result)>=100: result = result[:100]
        #if type == None: result = [{'label':x["label"],'value':x["value"],'type':x["type"]} for x in result] #only extract type, value & label
        results[key] = result
    return results
