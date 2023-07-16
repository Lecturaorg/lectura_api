from fastapi import FastAPI, Response, Request
import json
from sql_funcs import engine, read_sql
import pandas as pd
import numpy as np
from main_data import mainData
from sqlalchemy import text
from urllib.parse import parse_qs
import bcrypt

app = FastAPI()
@app.get("/data")
def data(response: Response, type = None, id:int = None, by = None):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    if (type != None and id != None):
        if type == 'authors':
            query = f"select * from authors where author_id = '{str(id)}'"
            author = pd.read_sql(query,con=engine()).replace(np.nan,None).to_dict('records')[0]
            return author
        if type == 'texts':
            if by == "author":
                query = f'''SET statement_timeout = 60000; 
                            select text_id
                                ,text_title as "titleLabel"
                                ,text_author
                                ,text_q
                                ,text_title || 
                                case
                                    when text_original_publication_year is null then ' ' 
                                    when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') '
                                    else ' (' || text_original_publication_year || ' AD' || ') '
                                end as "bookLabel"
                            from texts where author_id = '{str(id)}' '''
                texts = pd.read_sql(query, con=engine()).replace(np.nan, None).to_dict('records')
            else:
                query = f"select * from texts where text_id = '{str(id)}' "
                texts = pd.read_sql(query, con=engine()).replace(np.nan, None).to_dict('records')[0]#.to_json(orient="table")
            return texts
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
            queryBase = '''SET statement_timeout = 60000;
                select  * from authors WHERE  '''
            variables = searchtype.replace("s","")+"_id"
            filterString = ""
            whereClause = "WHERE "
            for n in range(len(filters)): #varlist should be a body in API request and optional
                var = filters[n]
                variables += ","+ var["value"] + '''::varchar(255) "''' + var["label"] + '''" \n''' #Add every search variable
                if n == len(filters)-1: filterString+= var["value"] + "::varchar(255) ILIKE '%" + query + "%'"
                else: filterString += var["value"] + "::varchar(255) ILIKE '%" + query + "%'" + " OR \n"
            query = queryBase.replace("*", variables).replace("WHERE ","WHERE " + filterString).replace("authors",str(searchtype))
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
    query = f"SELECT * FROM users WHERE user_email = '{email}' or user_name = '{username}'"
    df = pd.read_sql_query(query, conn)
    if not df.empty: 
        response.body = json.dumps({"message": "Duplicate"}).encode("utf-8")
        response.status_code = 200
    else:
        hashedPassword = reqInfo["user_password"]
        conn.execute("INSERT INTO USERS (user_name, user_email, hashed_password) VALUES (%s, %s, %s)", (username, email, hashedPassword))
        response.body = json.dumps({"user_id": pd.read_sql(query, conn).to_dict("records")[0]["user_id"]}).encode("utf-8")#.user_id)#return pd.read_sql(query,conn).to_dict("records")[0].user_id
        response.status_code = 200
        conn.close()
    return response

@app.get("/login_user")
def login(response:Response, user):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if "@" in user: login_col = "user_email"
    else: login_col = "user_name"
    conn = engine().connect()
    query = "SELECT user_id, user_name, user_email, hashed_password from USERS where %s = '%s'" % (login_col, user.lower())
    df = pd.read_sql_query(query, conn)
    if df.empty: return False
    else:
        df = df.to_dict('records')[0]
        return {"pw":df["hashed_password"].tobytes().decode('utf-8')
                    ,"user_id":df["user_id"], "user_name":df["user_name"],"user_email":df["user_email"]}

@app.post("/delete_user")
async def delete_user(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    response.headers['Content-Type'] = 'application/json'
    reqInfo = await info.json()
    conn = engine().connect()
    conn.execute("UPDATE USERS SET HASHED_PASSWORD = NULL, USER_EMAIL = NULL, USER_NAME = '(deleted)_%s' WHERE USER_ID = %s" % (reqInfo["user_name"], reqInfo["user_id"]))
    conn.close()

@app.post("/create_list")
async def createList(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    user_id = reqInfo["user_id"]
    list_name = reqInfo["list_name"]
    list_descr = reqInfo["list_description"]
    list_type = reqInfo["list_type"]
    conn = engine().connect()
    checkIfExists = "SELECT list_id from USER_LISTS where list_name = '%s'" % (list_name)
    if pd.read_sql(checkIfExists, conn).empty:
        conn.execute("INSERT INTO USER_LISTS (user_id, list_name, list_description, list_type) VALUES (%s, %s, %s, %s)",(user_id, list_name, list_descr, list_type))
        list_id = pd.read_sql("SELECT list_id FROM USER_LISTS where list_name = '%s'" % (list_name), conn).to_dict("records")[0]["list_id"]
        conn.close()
        response.body = json.dumps({"list_id":list_id}).encode("utf-8")
        response.status_code = 200
        return response

@app.get("/get_user_list")
def get_user_list(response:Response, list_id:int, user_id:int=None):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if list_id>0: query = "SELECT L.*,u.user_name FROM USER_LISTS L join USERS u on u.user_id=l.user_id WHERE LIST_ID = '%s'" % list_id
    else: query = "SELECT L.* FROM OFFICIAL_LISTS L WHERE LIST_ID = '%s'" % abs(list_id)
    lists = pd.read_sql(query, con=engine())
    if user_id:
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
        else: lists = pd.merge(lists, list_interactions, how="left",on="list_id")
    if user_id is None: user_id = 'null'
    else: user_id = user_id
    if lists.empty: return False
    else: 
        list_info = lists.to_dict('records')[0]
        if list_info["list_type"] == "authors": detail_query = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/list_elements_authors.sql")
        elif list_info["list_type"] == "texts": 
            detail_query = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/list_elements_texts.sql")
            detail_query = detail_query.replace('[$user_id]',str(user_id))
        list_elements = pd.read_sql(detail_query.replace("[@list_id]",str(list_id)), con=engine()).fillna('').to_dict('records')
        data = {"list_info": list_info, "list_detail": list_elements}
        return data

@app.post("/update_user_list")
async def update_user_list(response:Response, info:Request): #Update every list_info component, remove removed elements, add new ones
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    list_info = reqInfo["list_info"]
    list_id = list_info["list_id"]
    additions = reqInfo["additions"]
    removals = reqInfo["removals"]
    order_changes = reqInfo["order_changes"]
    delete = reqInfo["delete"]
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
            conn.execute(f"UPDATE USER_LISTS SET {element} = '{list_info[element]}' WHERE LIST_ID = {list_id}")
    if delete: conn.execute(f"UPDATE USER_LISTS SET LIST_DELETED = true WHERE LIST_ID = {list_id}")
    conn.execute(f"UPDATE USER_LISTS SET LIST_MODIFIED_DATE CURRENT_TIMESTAMP WHERE LIST_ID = {list_id}")
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.post("/user_list_interaction")
async def user_list_interaction(response:Response, info:Request):
    response.headers["Access-Control-Allow-Origin"] = "*"
    reqInfo = await info.json()
    interaction_type = reqInfo["type"]
    list_id = reqInfo["list_id"]
    user_id = reqInfo["user_id"]
    delete = reqInfo["delete"]
    if not delete: query = "INSERT INTO USER_LISTS_%sS (list_id, user_id) VALUES (%s, %s)" % (interaction_type, list_id, user_id)
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
		,coalesce(cr.comment_likes,0) likes
		,coalesce(cr.comment_dislikes,0) dislikes
		,U.USER_NAME 
        ,cr_user.comment_rating_type as user_interaction
		FROM COMMENTS C JOIN USERS U ON U.USER_ID = C.USER_ID
		LEFT JOIN (
			select comment_id 
			 	,SUM(CASE WHEN comment_rating_type = 'like' then 1 else 0 end) comment_likes
			 	,SUM(CASE WHEN comment_rating_type = 'dislike' then 1 else 0 end) comment_dislikes			 
			 from comment_ratings group by comment_id) cr on cr.comment_id = c.comment_id
		left join (select comment_id, max(comment_rating_type) comment_rating_type 
				   from comment_ratings WHERE user_id={str(user_id)} group by comment_id) cr_user on cr_user.comment_id = c.comment_id
        WHERE COMMENT_TYPE = '{comment_type}' AND COMMENT_TYPE_ID = {comment_type_id}'''
    comments = pd.read_sql(query, con=engine()).replace(np.nan,None).to_dict('records')
    def create_comment_tree(comments, parent_id=None):
        tree = []
        for comment in comments:
            if comment['parent_comment_id'] == parent_id:
                comment['replies'] = create_comment_tree(comments, comment['comment_id'])
                tree.append(comment)
        return tree
    comment_tree = create_comment_tree(comments)
    print(comment_tree)
    return comment_tree

@app.post("/comment_interaction")
async def comment_interaction(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    interaction_type = reqInfo["type"]
    user_id = reqInfo["user_id"]
    comment_id = reqInfo["comment_id"]
    conn = engine().connect()
    if not interaction_type: conn.execute(f"DELETE FROM comment_ratings WHERE user_id = {user_id} and comment_id = {comment_id}")
    else: conn.execute(f'''
        DELETE FROM comment_ratings WHERE user_id = {user_id} and comment_id = {comment_id};
        INSERT INTO comment_ratings (user_id, comment_id, comment_rating_type) VALUES ({user_id},{comment_id},'{interaction_type}')
        ''')
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

@app.post("/text_interaction")
async def text_interaction(response:Response, info:Request):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    user_id = reqInfo["user_id"]
    text_id = reqInfo["text_id"]
    type = reqInfo["type"]
    condition = reqInfo["condition"]
    if not condition: query = f"DELETE FROM {type} WHERE USER_ID = {user_id} AND TEXT_ID = {text_id};"
    else: query = f'''DELETE FROM {type} WHERE USER_ID = {user_id} AND TEXT_ID = {text_id};
                        INSERT INTO {type} (text_id, user_id) VALUES ({text_id},{user_id});'''
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response