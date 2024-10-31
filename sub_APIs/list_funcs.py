from sql_funcs import validateUser, engine, read_sql
import pandas as pd
import json

def postUpdates(changes,list_id):
    list_info = changes["list_info"]
    additions = changes["additions"]
    removals = changes["removals"]
    order_changes = changes["order_changes"]
    if "delete" in changes.keys(): delete = changes["delete"]
    else: delete = None
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
                conn.execute(f"UPDATE USER_LISTS SET {element} = %s WHERE LIST_ID = %s",(list_info[element], list_id))
    if delete: conn.execute(f"UPDATE USER_LISTS SET LIST_DELETED = true WHERE LIST_ID = {list_id}")
    conn.execute(f"UPDATE USER_LISTS SET LIST_MODIFIED = CURRENT_TIMESTAMP WHERE LIST_ID = {list_id}")
    conn.close()

async def create_list_func(response, info):
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

def get_user_list_func(response, list_id, user_id, hash):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if list_id>0: 
        list_type = "user"
        addon=""
        multiplier = 1
    else: 
        list_type= "official"
        addon=",list_url"
        multiplier = -1
    if list_type=="official": private = ",false as list_private\n,false as list_deleted"
    else: private = ",l.list_private\n,list_deleted"
    query = f'''SELECT 	
                    l.list_id*{multiplier} list_id
                    ,l.list_name
                    ,l.list_description
                    ,l.list_type
                    ,l.user_id
                    {private}
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

def get_user_lists_elements(response, list_type, type_id, user_id, hash):
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

async def update_user_list_func(response, info):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    if not validateUser(reqInfo["userData"]["user_id"], reqInfo["userData"]["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    list_id = reqInfo["list_info"]["list_id"]
    postUpdates(reqInfo, list_id)
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

def user_list_references_func(response, type, id):
    response.headers['Access-Control-Allow-Origin'] = "*"
    query = f'''SELECT l.*, u.user_name
            from user_lists l
            left join user_lists_elements e on e.list_id = l.list_id
            left join users u on u.user_id = l.user_id
            where l.list_type = '{type}s' and e.value = {id} '''
    lists = pd.read_sql(query, con=engine())
    if lists.empty: return {}
    else: return lists.fillna('').to_dict('records')

async def user_list_interactions_func(response, info):
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

def get_all_lists_func(response, user_id):
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