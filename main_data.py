from TABLE_MODELS import engine
import pandas as pd
import json
import numpy as np

eng = engine()
authorQuery = '''
select 
    a.author_id
    ,author_name
    ,author_positions
    ,author_name_language
    ,author_birth_date
    ,author_birth_city
    ,author_birth_country
    ,author_birth_coordinates
    ,author_birth_year
    ,author_birth_month
    ,author_birth_day
    ,author_death_date
    ,author_death_city
    ,author_death_country
    ,author_death_coordinates
    ,author_death_year
    ,author_death_month
    ,author_death_day
    ,author_nationality
    ,author_gender
    ,author_floruit
    ,text_ids
    ,text_titles
    ,text_publications
from authors a
left join (
	select author_id
		,GROUP_CONCAT(text_id SEPARATOR '|') as text_ids
		,GROUP_CONCAT(text_title SEPARATOR '|') as text_titles
		,GROUP_CONCAT(text_original_publication_year SEPARATOR '|') as text_publications
    from texts
    where author_id is not null
    group by author_id
 ) t on t.author_id = a.author_id
    '''

textQuery = '''
    select
t.text_id
,text_title
,text_author
,author_id author_id
,text_type
,text_genre
,text_language
,text_original_publication_date
,text_original_publication_year
,text_original_publication_month
,text_original_publication_day
,text_original_publication_publisher
,text_original_publication_publisher_loc
,text_original_publication_type
,text_original_publication_length
,text_original_publication_length_type
,text_writing_start
,text_writing_end
,edition_ids
,titles
,publication_years
,additional_authors
,languages
,isbn
,isbn13
from texts t
left join (
	select
		text_id
        ,GROUP_CONCAT(edition_id SEPARATOR '|') as edition_ids
        ,GROUP_CONCAT(edition_title SEPARATOR '|') as titles
        ,GROUP_CONCAT(edition_publication_year SEPARATOR '|') as publication_years
        ,GROUP_CONCAT(edition_editor SEPARATOR '|') as additional_authors
        ,GROUP_CONCAT(edition_language SEPARATOR '|') as languages
        ,GROUP_CONCAT(edition_isbn SEPARATOR '|') as isbn
        ,GROUP_CONCAT(edition_isbn13 SEPARATOR '|') as isbn13
        from editions
        group by text_id
) e on e.text_id = t.text_id
'''

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

texts = pd.read_sql(textQuery, con=eng).replace(np.nan, None).to_dict('records')#.to_json(orient="table")
#texts = texts.replace([np.inf, -np.inf], np.nan).to_dict('records')
authors = pd.read_sql(authorQuery, con=eng).replace(np.nan, None).to_dict('records')#.to_json(orient="table")
editions = pd.read_sql(editionQuery, con=eng).replace(np.nan, None).to_dict('records')#.to_json(orient="table")

#print(texts)
database = {'texts':texts, 'authors':authors, 'editions':editions}
with open("database_test.json", "w") as output_file: json.dump(database,output_file)







