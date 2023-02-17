from table_models import engine
import pandas as pd
import json
import numpy as np

def mainData(type = None):
    eng = engine()
    authorQuery = '''select * from AUTHORS_INCL_LABEL '''
    textQuery = '''select * from texts_incl_label'''
    editionQuery = '''select e.*, t.text_author, t.text_title from editions e
                    left join texts t on t.text_id = e.text_id'''
    if type == "all": 
        authorQuery = 'select * from authors'
        textQuery = 'select * from texts'
        editionQuery = '''select e.*, t.text_author, t.text_title 
                        from editions e
                        left join texts t on t.text_id = e.text_id'''
    texts = pd.read_sql(textQuery, con=eng).replace(np.nan, None).to_dict('records')
    authors = pd.read_sql(authorQuery, con=eng).replace(np.nan, None).to_dict('records')
    editions = pd.read_sql(editionQuery, con=eng).replace(np.nan, None).to_dict('records')
    database = {'texts':texts, 'authors':authors, 'editions':editions}
    return database
    #with open("database_test.json", "w") as output_file: json.dump(database,output_file)
 







