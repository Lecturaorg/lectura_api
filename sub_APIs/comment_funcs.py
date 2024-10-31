import json
from sql_funcs import engine, validateUser
import pandas as pd
async def upload_comment_func(response, info):
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
    if comment_type_id is None: comment_type_id = "null"
    query = '''INSERT INTO COMMENTS (user_id, comment_content, parent_comment_id, comment_type, comment_type_id) VALUES 
        (%s, '%s', %s, '%s', %s) ''' % (user_id, comment, parent_comment_id, comment_type, comment_type_id)
    conn = engine().connect()
    conn.execute(query)
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

async def update_comment_func(response, info):
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
    if delete or delete is False:
        conn.execute(f'''UPDATE COMMENTS SET COMMENT_EDITED_AT = CURRENT_TIMESTAMP, COMMENT_DELETED = {delete} WHERE COMMENT_ID = {comment_id}''') 
    else:
        new_content = '''UPDATE COMMENTS SET COMMENT_CONTENT = %s, COMMENT_EDITED_AT = CURRENT_TIMESTAMP WHERE COMMENT_ID = %s'''
        conn.execute(new_content, (comment, comment_id))
    conn.close()
    response.status_code = 200
    response.body = json.dumps(reqInfo).encode('utf-8')
    return response

def get_comments_func(response, comment_type, comment_type_id, user_id):
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
        ORDER BY comment_created_at desc '''
    if comment_type == "text": 
        query = query.replace("LEFT JOIN (", "LEFT JOIN TEXTS t on t.text_id = c.comment_type_id \n LEFT JOIN (").replace(",U.USER_NAME",",t.author_id \n ,U.USER_NAME")
    if comment_type_id == 'null': query = query.replace("COMMENT_TYPE_ID = null", "COMMENT_TYPE_ID is null")
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

async def comment_interactions_func(response, info):
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
