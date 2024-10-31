from sql_funcs import engine, validateUser, pd_dict
import json
import pandas as pd
#from main_data import profileViewData
import secrets
import requests
import hashlib

def profileViewData(user_id):
    author_watch = f'''SELECT a.author_id
                        ,replace(author_q, 'http://www.wikidata.org/entity/', '') author_q
                FROM author_watch a
                left join authors a2 on a2.author_id = a.author_id
                where a.user_id={user_id} ''' #author_watch, checks, watch
    text_watch = f'''SELECT t.text_id
                ,replace(text_q, 'http://www.wikidata.org/entity/', '') text_q
                ,text_title
                ,author_id
                from watch w
                left join texts t on t.text_id = w.text_id
                where w.user_id = {user_id} '''
    user_lists_watchlists = f'''SELECT w.list_id
                ,l.list_name
                ,l.list_description
                ,l.list_type
                ,l.list_created
                ,u.user_name
                from user_lists_watchlists w
                left join user_lists l on l.list_id = w.list_id
                left join users u on u.user_id = w.user_id
                where w.user_id = {user_id} and l.list_deleted is not true'''
    checks = f'''SELECT c.text_id
                ,replace(text_q, 'http://www.wikidata.org/entity/', '') text_q
                ,text_title
                ,author_id
                ,check_date::date check_date
                from checks c
                left join texts t on t.text_id = c.text_id
                where c.user_id = {user_id} '''
    comments = f'''SET statement_timeout = 60000;SELECT c.comment_id
                ,c.comment_content
                ,c.comment_type
                ,c.comment_type_id
                ,c.comment_created_at::date comment_created_at
                ,case when c.comment_type = 'text' then t.author_id else null end as author_id
                ,case when c.comment_type = 'list' then l.list_name else null end as list_name
                ,case 
                    when c.comment_type = 'list' then l.list_name
                    when c.comment_type = 'text' then text_title || 
                            case
                                when text_original_publication_year is null then ' - ' 
                                when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
                                else ' (' || text_original_publication_year || ' AD' || ') - '
                            end || coalesce(text_author,'Unknown')
                    when c.comment_type = 'author' then CONCAT(
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
                            END,''))
                end as comment_label
                ,c2.comment_content as commented_comment
                from comments c
                left join texts t on c.comment_type_id = text_id
                left join authors a on a.author_id = c.comment_type_id
                left join user_lists l on l.list_id = c.comment_type_id
                left join comments c2 on c2.comment_id = c.parent_comment_id
                where c.user_id = {user_id} and c.comment_deleted is not true '''
    lists = f'''SELECT l.list_id
                ,l.list_name
                ,l.list_description
                ,l.list_type
                ,l.list_created
                ,u.user_name
                from user_lists l
                left join users u on u.user_id = l.user_id
                where l.user_id = {user_id} and l.list_deleted is not true '''
    favorites = f'''SELECT f.text_id
                ,replace(text_q, 'http://www.wikidata.org/entity/', '') text_q
                ,author_id
                from favorites f
                left join texts t on t.text_id = f.text_id
                where f.user_id = {user_id} '''
    dislikes = f'''SELECT d.text_id
                ,replace(text_q, 'http://www.wikidata.org/entity/', '') text_q
                ,author_id
                from dislikes d
                left join texts t on t.text_id = d.text_id
                where d.user_id = {user_id} '''
    list_favorites = f'''SELECT l.list_id
                ,l.list_name
                ,l.list_description
                ,l.list_type
                ,l.list_created
                ,u.user_name
                from user_lists_likes lik
                left join user_lists l on l.list_id = lik.list_id
                left join users u on u.user_id = l.user_id
                where lik.user_id = {user_id} and l.list_deleted is not true '''
    list_dislikes = f'''SELECT l.list_id
                ,l.list_name
                ,l.list_description
                ,l.list_type
                ,l.list_created
                ,u.user_name
                from user_lists_dislikes dislik
                left join user_lists l on l.list_id = dislik.list_id
                left join users u on u.user_id = l.user_id
                where dislik.user_id = {user_id} and l.list_deleted is not true '''
    return {"author_watch":pd_dict(author_watch), "watch":pd_dict(text_watch)
            , "user_lists_watchlists":pd_dict(user_lists_watchlists),"checks":pd_dict(checks)
            ,"comments":pd_dict(comments), "lists":pd_dict(lists), "favorites":pd_dict(favorites), "dislikes":pd_dict(dislikes)
            ,"list_favorites":pd_dict(list_favorites), "list_dislikes":pd_dict(list_dislikes)}

async def update_user_func(response, info):
    reqInfo = await info.json()
    change_type = reqInfo["change_type"]
    change_value = reqInfo["change_value"]
    user_id = reqInfo["user_id"]
    query = f'''UPDATE USERS SET {change_type} = '{change_value}' WHERE user_id = {user_id}'''
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

async def update_user_role_func(response, info):
    reqInfo = await info.json()
    change_user = reqInfo["change_user"]
    new_role = reqInfo["new_role"]
    query = f"UPDATE USERS SET USER_ROLE = '{new_role}' WHERE USER_ID = {change_user}"
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

async def create_user_func(response, info):
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

def login_func(response, info, user):
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

async def delete_user_func(response, info):
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

def get_user_updates_func(response, user_id, length, update_type):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if length == 'null': list_len = 10
    else: list_len = length
    if user_id == 'null': user = 'w.user_id is not null'
    else: user = 'w.user_id = ' + user_id
    if update_type == 'all': upd_type = 'w.type is not null'
    else: upd_type = "w.type = '" + update_type + "'"
    query = f'''
        select coalesce(w.author_id::varchar(255),t.author_id::varchar(255)) as author_id
        ,w.text_id::varchar
        ,t.text_title
        ,a.author_name
        ,w.user_id::varchar
        ,u.user_name
        ,w.up_date
        ,CONCAT(
        CASE WHEN NOW()::date-up_date::date>0 THEN CONCAT(NOW()::date-up_date::date, ' days ') ELSE '' END, 
        EXTRACT(HOUR FROM AGE(NOW(), up_date)), ' hours ago'
        ) date_diff
        ,w.type
        from
        ((select null as author_id,w.text_id, w.user_id, watch_date as up_date, 'watchlisted' as type
        from watch w
        order by watch_date desc
        LIMIT {list_len})
        UNION all
        (select author_id, null as text_id, user_id, watch_date as up_date, 'watchlisted' as type
        from author_watch
        order by watch_date desc
        LIMIT {list_len})
        UNION ALL
        (select null as author_id, text_id, user_id, check_date as upd_date, 'checked' as type 
        from checks
        order by check_date desc
        limit {list_len})
        UNION ALL
        (select null as author_id, text_id, user_id, interaction_date as upd_date, 'favorited' as type 
        from favorites
        order by interaction_date desc
        limit {list_len})
        UNION ALL
        (select null as author_id, text_id, user_id, interaction_date as upd_date, 'disliked' as type 
        from dislikes
        order by interaction_date desc limit {list_len})) as w
        left join texts t on t.text_id = w.text_id
        left join authors a on a.author_id = w.author_id
        left join users u on u.user_id = w.user_id
        where {user} and {upd_type}
        order by up_date desc
        LIMIT {list_len};
        '''
    df = pd.read_sql(query, con=engine()).to_dict('records')
    return df

def user_data_func(response, user_id):
    response.headers['Access-Control-Allow-Origin'] = "*"
    return profileViewData(user_id)

async def update_user_data_func(response, info):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    if not validateUser(reqInfo["user_id"], reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    return update_user(response, info)

async def update_user_role_func(response, info):
    response.headers['Access-Control-Allow-Origin'] = "*"
    reqInfo = await info.json()
    if not validateUser(reqInfo["user_id"], reqInfo["hash"]): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response
    return await update_user.update_user_role_func(response, info)

def admin_data_func(response, user_id, hash, data_type):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if not validateUser(user_id, hash): 
        response.body = json.dumps({"error":"user is not validated"}).encode("utf-8")
        response.status_code = 400
        return response    
    if data_type=="users":
        query = '''SELECT user_name, user_role, user_id, user_created from users where user_name not ilike '%%deleted%%' and user_role != 'administrator' '''
        return pd_dict(query)
