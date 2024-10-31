from fastapi import FastAPI, Response, Request
import uvicorn
import os
from sub_APIs import update_user, page_data, browse_func, official_lists, search_func, list_funcs, comment_funcs, externals

app = FastAPI()
@app.get("/")
def read_root():
    return {"message": "Hello, world!"}

if __name__ == "__main__":
    uvicorn.run("API:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

@app.get("/data")
def data(response: Response, type = None, id:int = None, by = None, user_id:int = 0):
    '''Extracts data for a particular author and/or text page:
        @type: 'authors' or 'texts'
        @id: id of author or text
        @by: none if specific author or text data, when 'author' it extracts all texts for a particular author id
        @user_id: user_id if a user is online, extracts interaction data
    '''
    return page_data.page_data(response, type, id, by, user_id)

@app.post("/get_texts")
async def get_texts(response:Response,info:Request):
    '''Gets text data for a particular author IDs'''
    return await page_data.texts_data(response, info)

@app.post("/delete_data")
async def delete_data(response:Response,info:Request):
    '''Deletes data for a page (i.e. author or text) if admin'''
    return await page_data.delete_data_func(response, info)

@app.post("/element_interaction")
async def element_interactions(response:Response, info:Request):
    '''Adds or removes an interaction to a particular page, i.e. favoriting a text, watchlisting an author'''
    return await page_data.element_interactions_func(response, info)

@app.get("/get_interactions")
def get_interactions(response:Response, type:str, id:int, detailed:bool=None):
    '''Extracts interactions to a particular page'''
    return page_data.get_interactions_func(response, type, id, detailed)

@app.post("/browse")
async def browse(response:Response, info:Request):
    '''Provides data for the browsing page'''
    return await browse_func.browse_func(response, info)

@app.get("/filters")
def filters(response: Response, filter_type:str):
    '''Builds filter options for the browsing page'''
    return browse_func.filters_func(response, filter_type)

@app.get("/labels")
def labels(response: Response, lang:str = None):
    '''Extracts page labels depending on browser language/selected option by user'''
    return browse_func.labels_func(response, lang)

@app.get("/official_lists")
def extract_list(response:Response, language=None, country=None, query_type=None):
    '''Extracts data for official lists, i.e. lists generated automatically by the system based on pre-defined criterias:
        - Number of texts by author
        - Authors without registered texts
    '''
    return official_lists.official_lists(response, language, country, query_type)

@app.get("/search")
def search(info: Request,response: Response, query, searchtype = None):
    '''Finds authors and/or texts matching the query and searchtype
        @query: search string
        @searchtype: authors/texts
    '''
    return search_func.search_func(info, response, query, searchtype)

@app.post("/create_user")
async def create_user(response:Response,info:Request):
    '''Creates a new user and inserts new profile into the database'''
    return await update_user.create_user_func(response,info)

@app.get("/login_user")
def login(response:Response,request:Request, user):
    '''Logs in user and adds a hashed session ID in the database'''
    return update_user.login_func(response, request, user)

@app.post("/delete_user")
async def delete_user(response:Response, info:Request):
    '''Deletes user and user session'''
    return update_user.delete_user_func(response, info)

@app.get("/admin_data")
def admin_data(response:Response, user_id:int, hash:str, data_type:str):
    return update_user.admin_data_func(response, user_id, hash, data_type)

@app.post("/update_user_role")
async def update_user_role(response:Response, info:Request):
    return await update_user.update_user_role_func(response, info)

@app.get("/user_data")
def user_data(response:Response, user_id:int):
    return update_user.user_data_func(response, user_id)

@app.post("/update_user_data")
async def update_user_data(response:Response, info:Request):
    return await update_user.update_user_data_func(response, info)

@app.get("/get_user_updates")
def get_user_updates(response:Response, user_id:str=None, length:int=None, update_type:str=None):
    return update_user.get_user_updates_func(response, user_id, length, update_type)

@app.post("/create_list")
async def create_list(response:Response, info:Request):
    '''Creates a new list by type, list name, description, etc'''
    return await list_funcs.create_list_func(response, info)

@app.get("/get_user_list")
def get_user_list(response:Response, list_id:int, user_id:int=None, hash:str=None):
    '''Gets data for a particular user list, user interactions if user id matches user list with the right hashing'''
    return list_funcs.get_user_list_func(response, list_id, user_id, hash)

@app.get("/get_element_user_lists")
def get_element_user_lists(response:Response, list_type:str, type_id:int,user_id:int=None, hash:str=None):
    '''Imports elements from a user_lists'''
    return list_funcs.get_user_lists_elements(response, list_type, type_id, user_id, hash)

@app.post("/update_user_list")
async def update_user_list(response:Response, info:Request): 
    '''Update every list_info component, remove removed elements, add new ones'''
    return await list_funcs.update_user_list_func(response, info)

@app.get("/user_list_references")
def user_list_references(response:Response, type:str, id:int):
    return list_funcs.user_list_references_func(response, type, id)

@app.post("/user_list_interaction")
async def user_list_interactions(response:Response, info:Request):
    '''Extracts user list interactions for the user'''
    return await list_funcs.user_list_interactions_func(response, info)

@app.get("/get_all_lists")
def get_all_lists(response:Response,user_id:int = None):
    '''Extracts all user lists (not their elements)'''
    return list_funcs.get_all_lists_func(response, user_id)

@app.post("/upload_comment")
async def upload_comment(response:Response, info:Request):
    '''Uploads new comment created by a user'''
    return await comment_funcs.upload_comment_func(response, info)

@app.post("/update_comment")
async def update_comment(response:Response, info:Request):
    '''Updates comment and adds edit dates'''
    return await comment_funcs.update_comment_func(response, info)

@app.get("/extract_comments")
def get_comments(response:Response, comment_type, comment_type_id, user_id:int=None):
    '''Extracts all comments of a particular location
        @comment_type: text/author/blog/list'''
    return comment_funcs.get_comments_func(response, comment_type, comment_type_id, user_id)

@app.post("/comment_interaction")
async def comment_interactions(response:Response, info:Request):
    '''Extracts comment interactions for the user if validated'''
    return await comment_funcs.comment_interactions_func(response, info)

@app.get("/source_data")
def source_data(response:Response, author:str, title:str,label:str, type:str):
    '''Extracts data from external sources - currently only BNF (Bibliothèque Nationale Française)'''
    return externals.source_data_func(response, author, title, label, type)