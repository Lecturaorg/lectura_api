from sql_funcs import pd_dict, engine, validateUser
from main_data import mainData
import pandas as pd
import json
from sqlalchemy import text

def page_data(type, id, by, user_id):
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

async def texts_data(response, info):
    response.headers['Access-Control-Allow-Origin'] = "*"
    response.headers['Content-Type'] = 'application/json'
    reqInfo = await info.json()
    authors = reqInfo["authors"]
    authors = ", ".join([str(num) for num in authors])
    query = f'''SET statement_timeout = 60000;
                SELECT text_id
                        ,text_title
                        ,text_author
                        ,author_id
                        ,text_type
                        ,text_language
                        ,text_original_publication_date
                        ,text_original_publication_year
                        ,text_original_publication_month
                        ,text_original_publication_day
                        ,text_q
                        ,text_author_q
                        ,text_description
                FROM texts t where t.author_id in ({authors})'''
    texts = pd.read_sql(query,con=engine()).drop_duplicates()
    response.body = texts.to_json(orient='records').encode("utf-8")
    response.status_code = 200
    return response

async def delete_date_func(response, info):
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

async def element_interactions_func(response, info):
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

def get_interactions_func(response, type, id, detailed):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if detailed:
        if type in ["checks", "watch", "favorites","dislikes"]: element_type = "text"
        elif type in ["user_lists_likes", "user_lists_dislikes","user_lists_watchlists"]: element_type = "list"
        else: element_type = "author"
        query = f'''SELECT distinct user_name FROM {type} e
                    LEFT JOIN USERS U on U.USER_ID = e.USER_ID
                    WHERE e.{element_type}_ID = {id}
                    '''
        return pd_dict(query)
    else:
        if type=="text": 
            query = f'''SELECT 
                    (SELECT COUNT(*) FROM checks WHERE text_id = {id}) AS checks,
                    (SELECT COUNT(*) FROM watch WHERE text_id = {id}) AS watch,
                    (SELECT COUNT(*) FROM favorites WHERE text_id = {id}) AS favorites,
                    (SELECT COUNT(*) FROM dislikes WHERE text_id = {id}) AS dislikes
                        '''
            return pd.read_sql(text(query), con=engine()).iloc[0].to_dict()
