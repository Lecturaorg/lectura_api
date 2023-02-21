from fastapi import FastAPI, Response, Request
import json
from datetime import datetime
from validation import checkDuplicates, searchDict, mainKeys
from table_models import engine
import pandas as pd
import numpy as np
from main_data import mainData
from importAPI import approveImport, importData

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

@app.post("/new")
async def add_new(info:Request, response:Response, type):
    response.headers["Access-Control-Allow-Origin"] = "*"
    req_info = await info.json()
    cols = req_info.keys()
    vals = []
    for col in cols:
        val = req_info[col]
        if isinstance(val, str): vals.append("'" + val + "'")
        else: vals.append(str(val))
    query = 'insert into ' + type + ' (' + ", ".join(cols) + ') VALUES (' + ", ".join(vals) + ")"
    print(query)
    #conn = engine().connect()
    #conn.execute(query)
    #conn.close()

@app.post("/edit")
async def edit_data(info: Request, response: Response, type, id):
    response.headers["Access-Control-Allow-Origin"] = "*"
    req_info = await info.json()
    if type == "authors": idType = "author_id"
    elif type == "texts": idType = "text_id"
    else: idType = "edition_id"
    print(req_info)
    conn = engine().connect()
    for j in req_info.keys():
        if j == idType: continue
        if isinstance(req_info[j],int): setData = str(req_info[j])
        else: setData = "'" + (req_info[j]) + "'"
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
    data = importData(reqInfo)
    return {
        "status" : "SUCCESS",
        "data" : data
    }

@app.post("/import/approve")
async def importApproval(type, response: Response, info: Request):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    approvedData = await info.json()
    approveImport(approvedData, type)
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
