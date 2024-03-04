from fastapi import FastAPI, Response, Request
import json
from sql_funcs import engine, read_sql, validateUser, pd_dict
import pandas as pd
import numpy as np
import secrets
import hashlib
from main_data import mainData, parseXML, returnLabel, profileViewData
from sqlalchemy import text
from urllib.parse import parse_qs
import requests

app = FastAPI()
@app.get("/data")
def data(response: Response, type = None, id:int = None, by = None, user_id:int = 0):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    if (type != None and id != None):
        if type == 'authors':
            query = f'''select a.*, case when watch=1 then true else false end as author_watch
                        from authors a 
                        left join (select count(*) watch, author_id from author_watch where user_id = {user_id} group by author_id) aw on aw.author_id = a.author_id 
                        where a.author_id = '{str(id)}'
                        '''
            author = pd_dict(query)[0]
            return author
        if type == 'texts':
            if by == "author":
                query = f'''SET statement_timeout = 60000; 
                            select t.text_id
                                ,text_title as "titleLabel"
                                ,text_author
                                ,text_q
                                ,replace(a.author_q, 'http://www.wikidata.org/entity/','') as author_q
                                ,text_title || 
                                case
                                    when text_original_publication_year is null then ' ' 
                                    when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') '
                                    else ' (' || text_original_publication_year || ' AD' || ') '
                                end as "bookLabel"
                                ,case when c.text_id is not null then true else false end as checks
                                ,case when w.text_id is not null then true else false end as watch
                                ,case when f.text_id is not null then true else false end as favorites
                                ,case when d.text_id is not null then true else false end as dislikes
                            from texts t
                            left join authors a on a.author_id::integer = t.author_id::integer
                            left join (select distinct text_id from checks where user_id = {user_id}) c on c.text_id = t.text_id
                            left join (select distinct text_id from watch where user_id = {user_id}) w on w.text_id = t.text_id
                            left join (select distinct text_id from favorites where user_id = {user_id}) f on f.text_id = t.text_id
                            left join (select distinct text_id from dislikes where user_id = {user_id}) d on d.text_id = t.text_id
                            where t.author_id = '{str(id)}' '''
                texts = pd_dict(query)
            else:
                query = f'''select t.*
                            ,case when c.text_id is not null then true else false end as checks
                            ,case when w.text_id is not null then true else false end as watch
                            ,case when f.text_id is not null then true else false end as favorites
                            ,case when d.text_id is not null then true else false end as dislikes
                            from texts t
                            left join (select distinct text_id from checks where user_id = {user_id}) c on c.text_id = t.text_id
                            left join (select distinct text_id from watch where user_id = {user_id}) w on w.text_id = t.text_id
                            left join (select distinct text_id from favorites where user_id = {user_id}) f on f.text_id = t.text_id
                            left join (select distinct text_id from dislikes where user_id = {user_id}) d on d.text_id = t.text_id
                            where t.text_id = '{str(id)}' '''
                texts = pd_dict(query)[0]#.to_json(orient="table")
            return texts
    else: results = mainData()
    return results

@app.get("/labels")
def data(response: Response, lang:str = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    query = f'''SELECT label_loc, label_value from labels where language = '{lang}' '''
    labels = pd.read_sql(query,con=engine()).drop_duplicates()
    labels = dict(zip(labels['label_loc'], labels['label_value']))
    return labels

@app.post("/delete_data")
async def delete_data(response:Response,info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    reqInfo = await info.json()
    data_type = reqInfo["type"]
    if data_type not in ["text", "author"]: return False
    id = reqInfo["id"]
    if reqInfo["deleted"] == True: deleted = False
    else: deleted = True
    if validateUser(reqInfo["user_id"], reqInfo["hash"]):
        query = f'''UPDATE {data_type}s SET {data_type}_deleted = {deleted} WHERE {data_type}_id = {id} '''
        conn = engine().connect()
        conn.execute(query)
        response.body = json.dumps(reqInfo).encode("utf-8")
        response.status_code = 200
        conn.close()


@app.get("/official_lists")
def extract_list(response:Response, language=None, country=None, query_type=None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    queries = {'num_books':"/Users/tarjeisandsnes/lectura_api/API_queries/texts_by_author.sql",
                'no_books':"/Users/tarjeisandsnes/lectura_api/API_queries/authors_without_text.sql"}
    query = read_sql(queries[query_type])
    if language=="All": language=""
    if country=="All": query = query.replace("a.author_nationality ilike '%[country]%' and ", "")
    query = query.replace("[country]", country.replace("'","''")).replace("[language]",language.replace("'","''"))
    results = pd.read_sql(text(query), con=engine())
    if query_type=="num_books": results = results.sort_values(by=["texts"],ascending=False)
    results = results.replace(np.nan, None).to_dict('records')
    return results

@app.get("/search")
def search(info: Request,response: Response, query, searchtype = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    params = info.query_params
    query = query.replace("'","''").strip()
    allQuery = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/search_all.sql")
    search_params = {"query": f"%{query}%"}
    if searchtype == None:
        queryList = query.split(' ')
        if len(queryList) == 1:
            results = pd.read_sql(allQuery,con=engine(),params=search_params).drop_duplicates().to_dict("records")
            return results
        else:
            results = False
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
            queryBase = f'''SET statement_timeout = 60000;
                select  * from {searchtype} WHERE  '''
            variables = searchtype.replace("s","")+"_id"
            if searchtype == "authors": variables+= ''',author_id as value,CONCAT(
                SPLIT_PART(author_name, ', ', 1),
                COALESCE(
                    CASE
                    WHEN author_birth_year IS NULL AND author_death_year IS NULL AND author_floruit IS NULL THEN ''
                    WHEN author_birth_year IS NULL AND author_death_year IS NULL THEN CONCAT(' (fl.', left(author_floruit,4), ')')
                    WHEN author_birth_year IS NULL THEN CONCAT(' (d.', 
                            CASE 
                                WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                                ELSE CONCAT(author_death_year::VARCHAR, ' AD')
                            END,
                        ')')
                    WHEN author_death_year IS NULL THEN CONCAT(' (b.', 
                        CASE
                            WHEN author_birth_year<0 THEN CONCAT(ABS(author_birth_year)::VARCHAR, ' BC')
                            ELSE concat(author_birth_year::VARCHAR, ' AD')
                        END,
                        ')')
                    ELSE CONCAT(' (', ABS(author_birth_year), '-',
                        CASE 
                            WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                            ELSE CONCAT(author_death_year::VARCHAR, ' AD')
                        END,
                        ')')
                    END,'')) AS label'''
            elif searchtype == "texts": variables+=''',text_id as value,text_title || 
                case
                    when text_original_publication_year is null then ' - ' 
                    when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
                    else ' (' || text_original_publication_year || ' AD' || ') - '
                end
                || coalesce(text_author,'Unknown')
                as label'''
            filterString = ""            
            if len(filters)>0:
                for n in range(len(filters)): #varlist should be a body in API request and optional
                    var = filters[n]
                    if var["value"]=="label": continue
                    variables += ","+ var["value"] + '''::varchar(255) "''' + var["value"] + '''" \n''' #Add every search variable
                    if n == len(filters)-1: filterString+= var["value"] + "::varchar(255) ILIKE '%" + query + "%'"
                    else: filterString += var["value"] + "::varchar(255) ILIKE '%" + query + "%'" + " OR \n"
            else:
                if searchtype == "authors": filterString += "author_name::varchar(255) ILIKE '%" + query + "%'"
                elif searchtype == "texts": 
                    filterString += "text_name::varchar(255) ILIKE '%" + query + "%'" + " OR \n text_author ILIKE '%" + query + "%'"
            query = queryBase.replace("*", variables).replace("WHERE ","WHERE " + filterString)
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

@app.post("/create_user")
async def createUser(response:Response,info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    response.headers['Content-Type'] = 'application/json'
    reqInfo = await info.json()
    email = reqInfo["user_email"].lower()
    username = reqInfo["user_name"].lower()
    conn = engine().connect()
    query = f"SELECT user_id FROM users WHERE user_email = '{email}' or user_name = '{username}'"
    df = pd.read_sql_query(query, conn)
    if not df.empty: 
        response.body = json.dumps({"message": "Duplicate"}).encode("utf-8")
        response.status_code = 200
    else:
        hashedPassword = reqInfo["user_password"]
        conn.execute("INSERT INTO USERS (user_name, user_email, hashed_password, user_role) VALUES (%s, %s, %s, %s)", (username, email, hashedPassword, 'basic'))
        response.body = json.dumps(profileViewData(pd.read_sql(query, conn).to_dict("records")[0]["user_id"])).encode("utf-8")
        response.status_code = 200
        conn.close()
    return response

@app.get("/login_user")
def login(response:Response,request:Request, user):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if "@" in user: login_col = "user_email"
    else: login_col = "user_name"
    query = "SELECT user_id, user_name,user_role, user_email, hashed_password from USERS where %s = '%s'" % (login_col, user)
    conn = engine().connect()
    df = pd.read_sql_query(query, conn)
    if df.empty: return False
    else:
        random_number = str(secrets.randbits(32))
        user_ip = request.client.host
        user_agent = request.headers.get('User-Agent')
        data = user_ip+user_agent+random_number
        hashed_data = hashlib.sha256(data.encode()).hexdigest()
        df = df.to_dict('records')[0]
        sessionQuery = f'''DELETE FROM USER_SESSIONS WHERE USER_ID = {df["user_id"]};
                        INSERT INTO USER_SESSIONS (HASH, USER_ID) VALUES ('{hashed_data}', {df["user_id"]})'''
        conn.execute(sessionQuery)
        return {"pw":df["hashed_password"].tobytes().decode('utf-8'),"hash":hashed_data
                ,"user_id":df["user_id"],"user_name":df["user_name"],"user_email":df["user_email"], "user_role":df["user_role"]}

@app.post("/delete_user")
async def delete_user(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    response.headers['Content-Type'] = 'application/json'
    reqInfo = await info.json()
    if not validateUser(reqInfo["user_id"], reqInfo["hash"]): return
    user_name = reqInfo["user_name"]
    user_id = reqInfo["user_id"]
    conn = engine().connect()
    conn.execute(f'''DELETE FROM DELETED_USERS WHERE USER_NAME = '{user_name}' and USER_ID = '{user_id}';
                    INSERT INTO DELETED_USERS (user_id, user_name, user_email, user_role, user_created, hashed_password) SELECT * FROM USERS WHERE USER_NAME = '{user_name}' AND USER_ID = '{user_id}';
                    DELETE FROM USER_SESSIONS WHERE USER_ID = '{user_id}';
                    DELETE FROM USERS WHERE USER_NAME = '{user_name}' and USER_ID = '{user_id}'; ''')
    conn.close()

@app.post("/create_list")
async def createList(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    print(reqInfo)
    params = reqInfo["list_info"]
    user_id = params["user_id"]
    hash = params["hash"]
    if not validateUser(user_id, hash): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    list_name = params["list_name"]
    list_descr = params["list_description"]
    list_type = params["list_type"]
    conn = engine().connect()
    checkIfExists = f"SELECT list_id from USER_LISTS where list_name = '{list_name}'"
    if pd.read_sql(checkIfExists, conn).empty:
        conn.execute(f"INSERT INTO USER_LISTS (user_id, list_name, list_description, list_type) VALUES ({user_id}, '{list_name}', '{list_descr}', '{list_type}')")
        list_id = pd.read_sql(f"SELECT list_id FROM USER_LISTS where list_name = '{list_name}'", conn).to_dict("records")[0]["list_id"]
        postUpdates(reqInfo,list_id)
        conn.close()
        response.body = json.dumps({"list_id":list_id}).encode("utf-8")
        response.status_code = 200
        return response

@app.get("/get_user_list")
def get_user_list(response:Response, list_id:int, user_id:int=None, hash:str=None):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if list_id>0: 
        list_type = "user"
        addon=""
        multiplier = 1
    else: 
        list_type= "official"
        addon=",list_url"
        multiplier = -1
    query = f'''SELECT 	
                    l.list_id*{multiplier} list_id
                    ,l.list_name
                    ,l.list_description
                    ,l.list_type
                    ,l.user_id
                    ,u.user_name 
                    ,coalesce(lik.likes,0) likes
                    ,coalesce(dis.dislikes,0) dislikes
                    ,coalesce(watch.watchlists,0) watchlists
                    {addon}
                FROM {list_type}_LISTS L 
                left join USERS u on u.user_id=l.user_id
                left join (select count(*) likes, list_id from user_lists_likes group by list_id) lik on lik.list_id = L.list_id*{multiplier}
                left join (select count(*) dislikes, list_id from user_lists_dislikes group by list_id) dis on dis.list_id = L.list_id*{multiplier}
                left join (select count(*) watchlists, list_id from user_lists_watchlists group by list_id) watch on watch.list_id = L.list_id*{multiplier}
                WHERE L.LIST_ID = {abs(list_id)}'''
    lists = pd.read_sql(query, con=engine())
    if validateUser(user_id, hash):
        interaction_query = f'''SELECT DISTINCT COALESCE(W.LIST_ID, L.LIST_ID, DL.LIST_ID) as list_id
            ,CASE WHEN W.LIST_ID IS NULL THEN FALSE ELSE TRUE END AS watchlist
            ,CASE WHEN L.LIST_ID IS NULL THEN FALSE ELSE TRUE END AS like
            ,CASE WHEN DL.LIST_ID IS NULL THEN FALSE ELSE TRUE END AS dislike
            from USER_LISTS_WATCHLISTS W 
            FULL JOIN USER_LISTS_LIKES L ON L.USER_ID = W.USER_ID AND L.LIST_ID = W.LIST_ID
            FULL JOIN USER_LISTS_DISLIKES DL ON DL.USER_ID = W.USER_ID AND DL.LIST_ID = W.LIST_ID
            WHERE W.USER_ID = '{str(user_id)}' OR L.USER_ID = '{str(user_id)}' OR DL.USER_ID = '{str(user_id)}' '''
        list_interactions = pd.read_sql(interaction_query, con=engine())
        if list_interactions.empty: lists = lists
        else: lists = pd.merge(lists, list_interactions, how="left",on="list_id").fillna('')
    if user_id is None: user_id = 'null'
    else: user_id = user_id
    if lists.empty: return False
    else: 
        list_info = lists.to_dict('records')[0]
        if list_info["list_type"] == "authors": detail_query = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/list_elements_authors.sql")
        elif list_info["list_type"] == "texts": detail_query = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/list_elements_texts.sql")
        else: detail_query = f'SELECT * FROM USER_LISTS_ELEMENTS WHERE LIST_ID = {list_id}'
        detail_query = detail_query.replace('[$user_id]',str(user_id))
        list_elements = pd.read_sql(detail_query.replace("[@list_id]",str(list_id)), con=engine())
        if not list_elements.empty: list_elements = list_elements.fillna('').to_dict('records')
        else: list_elements = []
        data = {"list_info": list_info, "list_detail": list_elements}
        return data

@app.get("/get_element_user_lists")
def get_element_user_lists(response:Response, list_type:str, type_id:int,user_id:int=None, hash:str=None):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if validateUser(user_id, hash):
        lists = f'''select distinct l.list_name
		,l.list_id
		,e.element_id
		,e.value
		,l.list_type
        ,true as in_list
        from user_lists l
        left join user_lists_elements e on e.list_id = l.list_id
        where l.list_deleted is not true and l.user_id = {user_id} and l.list_type = '{list_type}' and e.value = {type_id}
        union all
        select distinct list_name
		,l.list_id
		,0 as element_id
		,0 as value
		,l.list_type
        ,false in_list
        from user_lists l
        where l.list_deleted is not true and l.user_id = {user_id} and l.list_type = '{list_type}'
		and l.list_id not in (
			select distinct 
			l.list_id
        	from user_lists l
        	left join user_lists_elements e on e.list_id = l.list_id
        	where l.list_deleted is not true and l.user_id = {user_id} and l.list_type = '{list_type}' and e.value = {type_id}) '''
        df = pd.read_sql(lists, con=engine()).to_dict('records')
        return df

def postUpdates(changes,list_id):
    list_info = changes["list_info"]
    additions = changes["additions"]
    removals = changes["removals"]
    order_changes = changes["order_changes"]
    delete = changes["delete"]
    conn = engine().connect()
    if len(additions)>0:
        for element in additions: conn.execute("INSERT INTO USER_LISTS_ELEMENTS (list_id,value) VALUES (%s, %s)",(list_id, element["value"]))
    if len(removals)>0: 
        for element in removals: conn.execute("DELETE FROM USER_LISTS_ELEMENTS WHERE list_id = '%s' and value = '%s'" % (list_id, element["value"]))
    if len(order_changes)>0:
        for n in range(len(order_changes)): 
            conn.execute("UPDATE USER_LISTS_ELEMENTS SET ORDER_RANK = %s WHERE ELEMENT_ID = %s",(n, order_changes[n]["element_id"]))
    if not list_info is False and len(list_info.keys())>1:
        for element in list_info.keys():
            if element not in ["hash", "user_id"]:
                conn.execute(f"UPDATE USER_LISTS SET {element} = '{list_info[element]}' WHERE LIST_ID = {list_id}")
    if delete: conn.execute(f"UPDATE USER_LISTS SET LIST_DELETED = true WHERE LIST_ID = {list_id}")
    conn.execute(f"UPDATE USER_LISTS SET LIST_MODIFIED = CURRENT_TIMESTAMP WHERE LIST_ID = {list_id}")
    conn.close()


@app.post("/update_user_list")
async def update_user_list(response:Response, info:Request): #Update every list_info component, remove removed elements, add new ones
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    if not validateUser(reqInfo["userData"]["user_id"], reqInfo["userData"]["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    list_id = reqInfo["list_id"]
    postUpdates(reqInfo)
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.get("/user_list_references")
def user_list_references(response:Response, type:str, id:int):
    response.headers['Access-Control-Allow-Origin'] = "*"
    query = f'''SELECT l.*, u.user_name
            from user_lists l
            left join user_lists_elements e on e.list_id = l.list_id
            left join users u on u.user_id = l.user_id
            where l.list_type = '{type}s' and e.value = {id} '''
    lists = pd.read_sql(query, con=engine())
    if lists.empty: return {}
    else: return lists.fillna('').to_dict('records')

@app.post("/user_list_interaction")
async def user_list_interaction(response:Response, info:Request):
    response.headers["Access-Control-Allow-Origin"] = "*"
    reqInfo = await info.json()
    if not validateUser(reqInfo["user_id"], reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    interaction_type = reqInfo["type"]
    list_id = reqInfo["list_id"]
    user_id = reqInfo["user_id"]
    delete = reqInfo["delete"]
    if not delete: 
        query = f'''DELETE FROM USER_LISTS_{interaction_type}S WHERE LIST_ID = {list_id} and USER_ID = {user_id};
            INSERT INTO USER_LISTS_{interaction_type}S (list_id, user_id) VALUES ({list_id}, {user_id})'''
    else: query = "DELETE FROM USER_LISTS_%ss WHERE list_id = '%s' AND user_id = '%s'" % (interaction_type, list_id, user_id)
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.body = json.dumps(reqInfo).encode('utf-8')
    response.status_code = 200
    return response

@app.get("/get_all_lists")
def get_all_lists(response:Response,user_id:int = None):
    response.headers['Access-Control-Allow-Origin'] = "*"
    query = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/list_of_lists.sql")
    lists = pd.read_sql(query,con=engine())
    if user_id:
            interaction_query = f'''SELECT DISTINCT COALESCE(W.LIST_ID, L.LIST_ID, DL.LIST_ID) as list_id
                ,CASE WHEN W.LIST_ID IS NULL THEN FALSE ELSE TRUE END AS watchlist
                ,CASE WHEN L.LIST_ID IS NULL THEN FALSE ELSE TRUE END AS like
                ,CASE WHEN DL.LIST_ID IS NULL THEN FALSE ELSE TRUE END AS dislike
                from USER_LISTS_WATCHLISTS W 
                FULL JOIN USER_LISTS_LIKES L ON L.USER_ID = W.USER_ID AND L.LIST_ID = W.LIST_ID
                FULL JOIN USER_LISTS_DISLIKES DL ON DL.USER_ID = W.USER_ID AND DL.LIST_ID = W.LIST_ID
            WHERE W.USER_ID = '{user_id}' OR L.USER_ID = '{user_id}' OR DL.USER_ID = '{user_id}' '''
            list_interactions = pd.read_sql(interaction_query, con=engine())
            if list_interactions.empty: lists = lists
            else: lists = pd.merge(lists, list_interactions, how="left",on="list_id")
    lists = lists.replace(np.nan,None).to_dict('records')
    return lists

@app.post("/upload_comment")
async def upload_comment(response:Response, info:Request):
    response.headers["Access-Control-Allow-Origin"] = "*"
    reqInfo = await info.json()
    user_id = reqInfo["user_id"]
    if not validateUser(user_id, reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    comment = reqInfo["comment"]
    parent_comment_id = reqInfo["parent_comment_id"]
    if parent_comment_id is None: parent_comment_id = "null"
    comment_type = reqInfo["type"]
    comment_type_id = reqInfo["type_id"]
    query = '''INSERT INTO COMMENTS (user_id, comment_content, parent_comment_id, comment_type, comment_type_id) VALUES 
        (%s, '%s', %s, '%s', %s) ''' % (user_id, comment, parent_comment_id, comment_type, comment_type_id)
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.post("/update_comment")
async def update_comment(response:Response, info:Request):
    response.headers["Access-Control-Allow-Origin"] = "*"
    reqInfo = await info.json()
    if not validateUser(reqInfo["user_id"], reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response    
    comment_id = reqInfo["comment_id"]
    comment = reqInfo["comment"]
    delete = reqInfo["delete"]
    conn = engine().connect()
    if delete:
        conn.execute(f"UPDATE COMMENTS SET COMMENT_EDITED_AT = CURRENT_TIMESTAMP, COMMENT_DELETED = true WHERE COMMENT_ID = {comment_id}") 
    else: conn.execute(f"UPDATE COMMENTS SET COMMENT_CONTENT = '{comment}', COMMENT_EDITED_AT = CURRENT_TIMESTAMP WHERE COMMENT_ID = {comment_id}")
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.get("/extract_comments")
def comments(response:Response, comment_type, comment_type_id, user_id:int=None):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if user_id is None: user_id = 0
    else: user_id = user_id
    query = f'''SELECT C.comment_id
		,c.user_id
		,c.parent_comment_id
		,c.comment_type
		,c.comment_type_id
		,c.comment_content
		,c.comment_created_at
		,c.comment_edited_at
        ,c.comment_deleted
		,coalesce(cr.comment_likes,0) likes
		,coalesce(cr.comment_dislikes,0) dislikes
		,U.USER_NAME 
        ,cr_user.comment_rating_type as user_interaction
		FROM COMMENTS C JOIN USERS U ON U.USER_ID = C.USER_ID
		LEFT JOIN (select comment_id 
                        ,SUM(CASE WHEN comment_rating_type = 'like' then 1 else 0 end) comment_likes
                        ,SUM(CASE WHEN comment_rating_type = 'dislike' then 1 else 0 end) comment_dislikes			 
                    from comment_ratings group by comment_id) cr on cr.comment_id = c.comment_id
                    left join (select comment_id, max(comment_rating_type) comment_rating_type 
				   from comment_ratings WHERE user_id={str(user_id)} group by comment_id) cr_user on cr_user.comment_id = c.comment_id
        WHERE COMMENT_TYPE = '{comment_type}' AND COMMENT_TYPE_ID = {comment_type_id}
        ORDER BY comment_deleted desc '''
    if comment_type == "text": 
        query = query.replace("LEFT JOIN (", "LEFT JOIN TEXTS t on t.text_id = c.comment_type_id \n LEFT JOIN (").replace(",U.USER_NAME",",t.author_id \n ,U.USER_NAME")
    comments = pd.read_sql(query, con=engine()).replace(np.nan,None).to_dict('records')
    def create_comment_tree(comments, parent_id=None):
        tree = []
        for comment in comments:
            if comment['parent_comment_id'] == parent_id:
                comment['replies'] = create_comment_tree(comments, comment['comment_id'])
                tree.append(comment)
        return tree
    comment_tree = create_comment_tree(comments)
    return comment_tree

@app.post("/comment_interaction")
async def comment_interaction(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    if not validateUser(reqInfo["user_id"], reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    interaction_type = reqInfo["type"]
    user_id = reqInfo["user_id"]
    comment_id = reqInfo["comment_id"]
    conn = engine().connect()
    if not interaction_type: conn.execute(f"DELETE FROM comment_ratings WHERE user_id = {user_id} and comment_id = {comment_id}")
    else: conn.execute(f''' DELETE FROM comment_ratings WHERE user_id = {user_id} and comment_id = {comment_id};
                    INSERT INTO comment_ratings (user_id, comment_id, comment_rating_type) VALUES ({user_id},{comment_id},'{interaction_type}') ''')
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.post("/element_interaction")
async def element_interaction(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    user_id = reqInfo["user_id"]
    if not validateUser(user_id, reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response    
    type = reqInfo["type"]
    if type in ["checks", "watch", "favorites","dislikes"]: element_type = "text"
    else: element_type = "author"
    id = reqInfo["id"]
    condition = reqInfo["condition"]
    if not condition: query = f"DELETE FROM {type} WHERE USER_ID = {user_id} AND {element_type}_ID = {id};"
    else: query = f'''DELETE FROM {type} WHERE USER_ID = {user_id} AND {element_type}_ID = {id};
                        INSERT INTO {type} ({element_type}_id, user_id) VALUES ({id},{user_id});'''
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.get("/source_data")
def source_data(response:Response, author:str, title:str,label:str, type:str):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if type == "bnf":
        url = "http://gallica.bnf.fr/SRU"
        params = {
            "version": "1.2", "operation": "searchRetrieve",
            "query": f'''(dc.creator all "{author}") and (dc.title all "{title}")''',
            "startRecord": "1","maximumRecords": "20"
        }
        columns = ["creator", "date","description","language","publisher","source","title","type","subject","identifier"]
        response = requests.get(url, params=params)
        if response.status_code == 200:
            xml_data = response.content
            return parseXML(xml_data, columns)
        else: return response.status_code

@app.get("/admin_data")
def admin_data(response:Response, user_id:int, hash:str, data_type:str):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if not validateUser(user_id, hash): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response    
    if data_type=="users":
        query = '''SELECT user_name, user_role, user_id, user_created from users where user_name not ilike '%%deleted%%' and user_role != 'administrator' '''
        return pd_dict(query)

@app.post("/update_user_role")
async def update_user_role(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    change_user = reqInfo["change_user"]
    new_role = reqInfo["new_role"]
    if not validateUser(reqInfo["user_id"], reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    query = f"UPDATE USERS SET USER_ROLE = '{new_role}' WHERE USER_ID = {change_user}"
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.get("/user_data")
def user_data(response:Response, user_id:int):
    response.headers['Access-Control-Allow-Origin'] = "*"
    return profileViewData(user_id)

@app.post("/update_user_data")
async def update_user_data(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    change_type = reqInfo["change_type"]
    change_value = reqInfo["change_value"]
    user_id = reqInfo["user_id"]
    if not validateUser(user_id, reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    query = f'''UPDATE USERS SET {change_type} = '{change_value}' WHERE user_id = {user_id}'''
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response
