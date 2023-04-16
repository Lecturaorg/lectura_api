from fastapi import FastAPI, Response, Request
import json
from datetime import datetime
from validation import checkDuplicates, searchDict, mainKeys
from table_models import engine
import pandas as pd
import numpy as np
from main_data import mainData
from importAPI import approveImport, importData
from comments import import_comments
import struct
from sqlalchemy import text

app = FastAPI()

@app.get("/data")
def data(response: Response, type = None, id = None, by = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    if (type != None and id != None):
        if type == 'authors':
            query = '''select * from authors where author_id = ''' + "'" +id + "'"
            author = pd.read_sql(query,con=engine()).replace(np.nan,None).to_dict('records')[0]
            print(author)
            return author
        if type == 'texts':
            if by == "author":
                query = '''select 
                                text_id
                                ,text_title || 
                                case
                                    when text_original_publication_year is null then ' ' 
                                    when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') '
                                    else ' (' || text_original_publication_year || ' AD' || ') '
                                end as label
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
def search(response: Response, query, type = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    textQuery = '''select 
	text_id as value
    ,'text' as type
	,text_title || 
	case
		when text_original_publication_year is null then ' - ' 
		when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
		else ' (' || text_original_publication_year || ' AD' || ') - '
	end
	|| coalesce(text_author,'Unknown')
	as label
    from texts
    WHERE text_title ILIKE '%@query%'
        OR text_author ILIKE '%@query%'
        OR text_q ILIKE '%@query%'
        OR text_author_q ILIKE '%@query%'
    ORDER BY ts_rank_cd(to_tsvector('simple', text_title || ' ' || text_author
                                || ' ' || text_q
                                    || ' ' || text_author_q
                                ),plainto_tsquery('simple','query')) DESC'''
    authorQuery = '''SELECT 
	author_id as value
    ,'author' as type
	,CONCAT(
    SPLIT_PART(author_name, ', ', 1),
    COALESCE(
        CASE
        WHEN author_birth_year IS NULL AND author_death_year IS NULL AND author_floruit IS NULL THEN ''
        WHEN author_birth_year IS NULL AND author_death_year IS NULL THEN CONCAT(' (fl.', left(author_floruit,4), ')')
        WHEN author_birth_year IS NULL THEN CONCAT(' (d.', 
                CASE 
                    WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                    ELSE CONCAT(author_death_year::VARCHAR, ' AD')
				END
            ,
            ')')
        WHEN author_death_year IS NULL THEN CONCAT(' (b.', 
            CASE
				WHEN author_birth_year<0 THEN CONCAT(ABS(author_birth_year)::VARCHAR, ' BC')
            	ELSE concat(author_birth_year::VARCHAR, ' AD')
			END
            ,
            ')')
        ELSE CONCAT(' (', ABS(author_birth_year), '-',
            CASE 
				WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
            	ELSE CONCAT(author_death_year::VARCHAR, ' AD')
			END
            ,
            ')')
        END,
        ''
    )
    ) AS label
    FROM authors
    WHERE author_name ILIKE '%@query%'
    OR author_nationality ILIKE '%@query%'
    OR author_positions ILIKE '%@query%'
    OR author_birth_city ILIKE '%@query%'
    OR author_birth_country ILIKE '%@query%'
    OR author_name_language ILIKE '%@query%'
    OR author_q ILIKE '%@query%'
    ORDER BY ts_rank_cd(to_tsvector('simple', author_name || ' ' || author_nationality 
                                    || ' ' || author_positions
                                    || ' ' || author_birth_city
                                    || ' ' || author_birth_country
                                    || ' ' || author_name_language
                                    || ' ' || author_q
                                ), plainto_tsquery('simple', 'query')) DESC;'''
    if type == None: 
        queryList = query.split(' ')
        if len(queryList) == 1:
            texts = pd.read_sql(text(textQuery.replace("@query",queryList[0])), con=engine()).head(10)#.to_dict('records')
            authors = pd.read_sql(text(authorQuery.replace("@query",queryList[0])),con=engine()).head(10)
            results = pd.concat([texts, authors]).drop_duplicates().to_dict('records')
            return results
        else:
            texts = False; authors = False
            for subQuery in queryList:
                if isinstance(texts, pd.DataFrame) and isinstance(authors, pd.DataFrame):
                    newTexts = pd.read_sql(text(textQuery.replace("@query",subQuery)), con=engine())
                    texts = pd.merge(texts,newTexts, how="inner")
                    newAuthors = pd.read_sql(text(authorQuery.replace("@query",subQuery)),con=engine())
                    authors = pd.merge(authors, newAuthors, how="inner")
                else:
                    texts = pd.read_sql(text(textQuery.replace("@query",subQuery)), con=engine())
                    authors = pd.read_sql(text(authorQuery.replace("@query",subQuery)),con=engine())
            results = pd.concat([texts.head(5),authors.head(5)]).drop_duplicates().to_dict('records')
            return results
    else: keysToCheck = [type]
    data = mainData()
    results = {}
    for key in keysToCheck:
        dataToSearch = data[key]
        result = searchDict(dataToSearch, query)
        if len(result)>=100: result = result[:100]
        results[key] = result
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
