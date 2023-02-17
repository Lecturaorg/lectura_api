from fastapi import FastAPI, Response, Request
import json
from datetime import datetime
from validation import checkDuplicates, searchDict
from table_models import engine
import pandas as pd
import numpy as np
from main_data import mainData

app = FastAPI()

@app.get("/data")
def data(response: Response, type = None, id = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    if type != None and id != None:
        if type == 'texts':
            query = '''select * from getTexts(''' + id + ')' ##All texts of author_id = id
            texts = pd.read_sql(query, con=engine()).replace(np.nan, None).to_dict('records')#.to_json(orient="table")
            return texts
        if type == 'editions':
            query = '''select * from getEditions(''' + id + ')' #All editions of text_id = id
            editions = pd.read_sql(query, con = engine()).replace(np.nan,None).to_dict('records')
            return editions
    data = mainData()
    return data

@app.post("/edit")
async def edit_data(info: Request, response: Response, type, id):
    response.headers["Access-Control-Allow-Origin"] = "*"
    req_info = await info.json()
    if type == "authors": idType = "author_id"
    elif type == "texts": idType = "text_id"
    else: idType = "edition_id"
    conn = engine().connect()
    for j in req_info.keys():
        if j == idType: continue
        if req_info[j] is int: setData = str(req_info[j])
        else: setData = "'" + req_info[j] + "'"
        updateString = 'UPDATE ' + type + " SET " + j + " = " + setData + " WHERE " + idType + " = " + str(id)
        #insertDataString = '''INSERT INTO edits (id, type, variable, value) VALUES (%s, %s, %s, %s)''',(id, idType, j, req_info[j])
        conn.execute('''INSERT INTO edits (id, type, variable, value) VALUES (%s, %s, %s, %s)''',(id, idType, j, req_info[j]))
        conn.execute(updateString)
    conn.close()
    return {
        "status" : "SUCCESS",
        "data" : req_info
    }


@app.post("/import")
async def import_data(info: Request, response: Response):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    reqInfo = await info.json()
    importType = reqInfo["type"]
    fileName = importType+'_import.json'
    with open (fileName) as jsonFile: importData = json.load(jsonFile)
    date_uploaded = reqInfo["date_uploaded"]
    data = reqInfo["data"]
    databaseData = mainData(type="all")[importType]
    keysToAddIfNotExists = databaseData[0].keys()
    keys = {'texts': ["text_title", "text_author"]
            , "authors": ["author_name", "author_birth_year", "author_death_year"]
            , "editions":["edition_title", "edition_author"]} ##Keys to check
    keysToCheck = keys[importType]
    data = checkDuplicates(data,databaseData, keysToCheck = keysToCheck, importType = importType) ##Checks database if it exists
    if len(data)!=0: 
        data = checkDuplicates(data,importData, keysToCheck = keysToCheck) #Checks if import already exists
        combined = data+importData
    else: combined = importData
    with open(fileName, "w") as outfile: json.dump(combined, outfile)
    return {
        "status" : "SUCCESS",
        "data" : reqInfo
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
    data = mainData()
    results = {}
    if type == None: keysToCheck = ["authors", "texts", "editions"]
    else: keysToCheck = [type]
    for key in keysToCheck:
        dataToSearch = data[key]
        result = searchDict(dataToSearch, query)
        if len(result)>=100: result = result[:100]
        results[key] = result
    return results
