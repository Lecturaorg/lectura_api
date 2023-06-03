from fastapi import FastAPI, Response, Request
import json
from datetime import datetime
from validation import checkDuplicates, searchDict, mainKeys
from table_models import engine
import pandas as pd
import numpy as np
from main_data import mainData
from importAPI import approveImport, importData, read_sql
from comments import import_comments
import struct
from sqlalchemy import text
from urllib.parse import parse_qs
from sqlalchemy import text

app = FastAPI()

@app.get("/data")
def data(response: Response, type = None, id = None, by = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    if (type != None and id != None):
        if type == 'authors':
            query = '''select * from authors where author_id = ''' + "'" +id + "'"
            author = pd.read_sql(query,con=engine()).replace(np.nan,None).to_dict('records')[0]
            return author
        if type == 'texts':
            if by == "author":
                query = '''SET statement_timeout = 60000;select 
                                text_id
                                ,text_title as "titleLabel"
                                ,text_author
                                ,text_q
                                ,text_title || 
                                case
                                    when text_original_publication_year is null then ' ' 
                                    when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') '
                                    else ' (' || text_original_publication_year || ' AD' || ') '
                                end as "bookLabel"
                            from texts where author_id = ''' + "'" +id + "'"
                texts = pd.read_sql(query, con=engine()).replace(np.nan, None).to_dict('records')
            else:
                query = '''select * from texts where text_id = ''' + "'" +id + "'"#getTexts(''' + id + ')' ##All texts of author_id = id
                texts = pd.read_sql(query, con=engine()).replace(np.nan, None).to_dict('records')[0]#.to_json(orient="table")
            return texts
        if type == 'editions':
            query = '''select * from getEditions(''' + id + ')' #All editions of text_id = id
            editions = pd.read_sql(query, con = engine()).replace(np.nan,None).to_dict('records')
            return editions
    else: results = mainData()
    return results

@app.get("/lists")
def extract_list(response:Response, language=None, country=None, query_type=None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    queries = {'num_books':"/Users/tarjeisandsnes/lectura_api/API_queries/texts_by_author.sql",
                'no_books':"/Users/tarjeisandsnes/lectura_api/API_queries/authors_without_text.sql"}
    query = read_sql(queries[query_type])
    if language=="All": language=""
    if country=="All": query = query.replace("a.author_nationality ilike '%[country]%' and ", "")
    language = language.replace("'","''")
    country = country.replace("'","''")
    query = query.replace("[country]", country).replace("[language]",language)
    results = pd.read_sql(text(query), con=engine())
    if query_type=="num_books": results = results.sort_values(by=["texts"],ascending=False)
    results = results.replace(np.nan, None).to_dict('records')
    #print(results)
    return results


@app.post("/new")
async def add_new(info:Request, response:Response, type):
    response.headers["Access-Control-Allow-Origin"] = "*"
    req_info = await info.json()
    req_info = checkDuplicates([req_info], mainData()[type])
    if len(req_info) == 0: return ("This already exists on the database")
    else: 
        req_info = req_info[0]
        cols = req_info.keys()
        reqs = ["author_name", "text_title", "edition_title"]
        found = 0
        for req in reqs: 
            if req in cols: found+=1
        if found == 0: return "error"
        vals = []
        newCols = []
        for col in cols:
            val = req_info[col]
            if val == "": continue
            else: newCols.append(col)
            if isinstance(val, str):
                if val == "": val = None 
                if "'" in val:
                    val = val.replace("'", "''")
                vals.append("'" + val + "'")
            else: vals.append(str(val))
        query = 'insert into ' + type + ' (' + ", ".join(newCols) + ') VALUES (' + ", ".join(vals) + ")"
        print(query)
        conn = engine().connect()
        conn.execute(query)
        conn.close()

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
def search(info: Request,response: Response, query, searchtype = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    params = info.query_params
    query = query.replace("'","''")
    allQuery = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/search_all.sql")
    search_params = {"query": f"%{query}%"}
    if searchtype == None:
        queryList = query.split(' ')
        if len(queryList) == 1:
            results = pd.read_sql(allQuery,con=engine(),params=search_params).drop_duplicates().to_dict("records")
            return results
        else:
            results = False#texts = False; authors = False
            for subQuery in queryList:
                search_params["query"]=f"%{subQuery}%"
                if isinstance(results, pd.DataFrame):
                    newResults = pd.read_sql(allQuery,con=engine(),params=search_params)
                    results = pd.merge(results,newResults,how="inner")
                else: results = pd.read_sql(allQuery,con=engine(),params=search_params).drop_duplicates()
            results = results.to_dict('records'); #.head(5)
            return results
    else: ###Detailed search by type
        parsed = parse_qs(str(params))
        filters = json.loads(parsed.get('filters', [''])[0])
        def find_results(query):
            queryBase = '''
            SET statement_timeout = 60000;
            select 
                *
            from authors
            WHERE  
            '''
            variables = searchtype.replace("s","")+"_id"
            filterString = ""
            whereClause = "WHERE "
            for n in range(len(filters)): #varlist should be a body in API request and optional
                var = filters[n]
                variables += ","+ var["value"] + '''::varchar(255) "''' + var["label"] + '''" \n''' #Add every search variable
                if n == len(filters)-1: filterString+= var["value"] + "::varchar(255) ILIKE '%" + query + "%'"
                else: filterString += var["value"] + "::varchar(255) ILIKE '%" + query + "%'" + " OR \n"
            query = queryBase.replace("*", variables).replace("WHERE ","WHERE " + filterString).replace("authors",str(searchtype))
            print(query)
            results = pd.read_sql(text(query), con=engine()).drop_duplicates()#.to_dict('records')
            return results
        queryList = query.split(" ")
        if len(queryList) == 1: results = find_results(queryList[0]).to_dict('records')
        else:
            results = False
            for subQuery in queryList:
                if isinstance(results, pd.DataFrame):
                    newResults = find_results(subQuery)
                    results = pd.merge(results, newResults, how="inner").drop_duplicates()
                else: results = find_results(subQuery)
            results = results.to_dict('records')
        return results

@app.post("/login")
async def login(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    print(reqInfo["password"])
    return reqInfo

@app.post("/create_user")
async def createUser(response:Response,info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    hashedPassword = reqInfo["password"]["words"]
    passwordBytes = b''.join(struct.pack('!i',i) for i in hashedPassword)
    email = reqInfo["email"]
    conn = engine().connect()
    query = f"SELECT * FROM users WHERE email = '{email}'"
    df = pd.read_sql_query(query, conn)
    print(df)
    if not df.empty: return "duplicate"
    else:
        conn.execute("INSERT INTO users (hashed_password, email) VALUES (%s, %s)", (passwordBytes, email))
        conn.close()

@app.get("/extract_comments")
def comments(response:Response):
    response.headers['Access-Control-Allow-Origin'] = "*"
    comments = import_comments()
    return comments
