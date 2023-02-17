from table_models import engine
import pandas as pd
import json
import numpy as np

def mainData():
    eng = engine()
    authorQuery = '''select * from AUTHORS_INCL_LABEL '''
    textQuery = '''select * from texts_incl_label'''
    editionQuery = '''
    select 
    edition_id
    ,edition_title
    ,author_id
    ,text_id
    ,edition_editor
    ,edition_translator
    ,edition_publisher
    ,edition_publication_date
    ,edition_publication_year
    ,edition_isbn
    ,edition_isbn13
    ,edition_length
    ,edition_language
    ,edition_binding
    from editions
    '''
    texts = pd.read_sql(textQuery, con=eng).replace(np.nan, None).to_dict('records')
    authors = pd.read_sql(authorQuery, con=eng).replace(np.nan, None).to_dict('records')
    editions = pd.read_sql(editionQuery, con=eng).replace(np.nan, None).to_dict('records')
    database = {'texts':texts, 'authors':authors, 'editions':editions}
    return database
    #with open("database_test.json", "w") as output_file: json.dump(database,output_file)
 







