from table_models import engine
import pandas as pd
import json
import numpy as np
from sqlalchemy import text

def mainData(type = None, limit = None):
    eng = engine()
    authorQuery = '''
    SET statement_timeout = 60000; 
    select * from authors_incl_label
    ORDER BY author_added_date desc
    LIMIT 5;
    '''
    textQuery = '''SET statement_timeout = 60000; select * from texts_incl_label order by text_added_date desc LIMIT 5;'''
    #editionQuery = '''select e.*, t.text_author, t.text_title from editions e
     #               left join texts t on t.text_id = e.text_id'''
    if limit != None:
        authorQuery = authorQuery.replace("5",limit)
        textQuery = textQuery.replace("5",limit)
    if type == "all": 
        authorQuery = 'select * from authors '
        textQuery = 'select * from texts'
        editionQuery = '''select e.*, t.text_author, t.text_title 
                        from editions e
                        left join texts t on t.text_id = e.text_id'''
    texts = pd.read_sql(text(textQuery), con=eng).replace(np.nan, None).to_dict('records')
    authors = pd.read_sql(text(authorQuery), con=eng).replace(np.nan, None).to_dict('records')
    #editions = pd.read_sql(editionQuery, con=eng).replace(np.nan, None).to_dict('records')
    database = {'texts':texts, 'authors':authors}#, 'editions':editions}
    return database
    #with open("database_test.json", "w") as output_file: json.dump(database,output_file)
 







