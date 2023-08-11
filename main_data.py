from sql_funcs import engine
import pandas as pd
import numpy as np
from sqlalchemy import text
import xml.etree.ElementTree as ET

def mainData(type = None, limit = None):
    eng = engine()
    authorQuery = '''SET statement_timeout = 60000; select * from authors_incl_label ORDER BY author_added_date desc LIMIT 5;'''
    textQuery = '''SET statement_timeout = 60000; select * from texts_incl_label order by text_added_date desc LIMIT 5;'''
    if limit != None:
        authorQuery = authorQuery.replace("5",limit)
        textQuery = textQuery.replace("5",limit)
    if type == "all": 
        authorQuery = 'select * from authors'
        textQuery = 'select * from texts'
    texts = pd.read_sql(text(textQuery), con=eng).replace(np.nan, None).to_dict('records')
    authors = pd.read_sql(text(authorQuery), con=eng).replace(np.nan, None).to_dict('records')
    database = {'texts':texts, 'authors':authors}
    return database

def parseXML(xml, cols):
    root = ET.fromstring(xml)
    records = []
    record_elements = root.findall('.//{http://www.loc.gov/zing/srw/}record')
    for record_element in record_elements:
        record_dict = {}
        for col in cols:
            elements = record_element.findall('.//{http://purl.org/dc/elements/1.1/}'+col)
            if elements:
                creator_list = ' | '.join([element.text for element in elements])
                record_dict[col] = creator_list
        records.append(record_dict)
    return records
