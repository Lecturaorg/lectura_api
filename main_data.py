from sql_funcs import engine, pd_dict
import pandas as pd
import numpy as np
from sqlalchemy import text
import xml.etree.ElementTree as ET

def mainData(type = None, limit = None):
    eng = engine()
    authorQuery = '''SET statement_timeout = 60000; select * from authors_incl_label WHERE label is not null ORDER BY author_added_date desc LIMIT 5;'''
    textQuery = '''SET statement_timeout = 60000; select * from texts_incl_label WHERE label is not null and author_id is not null order by text_added_date desc LIMIT 5;'''
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

def returnLabel(type):
    if type=="author":
        return '''CONCAT(
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
                    END,'')) AS label '''
    elif type=="text":
        return '''text_title || 
                case
                    when text_original_publication_year is null then ' - ' 
                    when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
                    else ' (' || text_original_publication_year || ' AD' || ') - '
                end
                || coalesce(text_author,'Unknown')
                as label '''

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