from dotenv import load_dotenv
from TABLE_MODELS import Author, Text, Edition, engine
import json
from sqlalchemy.orm import sessionmaker
import sqlalchemy

Session = sessionmaker(bind=engine())

def stringToInt(data):
    if(data) == "": data = None
    else: data = data
    return data
def transformString(data):
    if data is None: return data
    elif isinstance(data,int): return data
    encodedBytes = data.encode('utf-8')
    decodedBytes = encodedBytes.decode('utf-8')
    return(decodedBytes)

def insertAuthors(data):
    conn = engine().connect()
    for i in data:
        name = transformString(i["name"])
        positions = transformString(i["position"])
        country = i["country"]
        city_death = transformString(i["city_death"])
        country_death = i["country_death"]
        nationality = transformString(i["nationality"])
        floruit = i["floruit"]
        birth = stringToInt(i["birth"])
        death = stringToInt(i["death"])
        conn.execute(
            """
            INSERT INTO authors (author_name, author_positions, author_birth_country, author_birth_year,
                                 author_death_city, author_death_country, author_nationality, author_floruit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (name, positions, country, birth, city_death, country_death, nationality, floruit)
        )
    conn.close()

def insertTexts(data):
    conn = engine().connect()
    for i in data:
        text_title = transformString(i["title"])
        text_author = transformString(i["author"])
        author_id = stringToInt(i["author_id"])
        text_language = stringToInt(i["language"])
        text_original_publication_year = stringToInt(i["publication"])
        text_original_publication_publisher = stringToInt(i["publisher"])
        text_original_publication_publisher_loc = stringToInt(i["publication_loc"])
        text_original_publication_type = stringToInt(i["publication_type"])
        text_original_publication_length = stringToInt(i["publication_length"])
        text_original_publication_length_type = stringToInt(i["publication_length_type"])
        text_writing_start = stringToInt(i["writing_start"])
        text_writing_end = stringToInt(i["writing_end"])
        conn.execute(
            '''
            INSERT INTO texts (text_title, text_author, author_id, text_language, text_original_publication_year,
                                text_original_publication_publisher, text_original_publication_publisher_loc,
                                text_original_publication_type, text_original_publication_length,
                                text_original_publication_length_type, text_writing_start, text_writing_end)
                            VALUES (%s, %s, %s, %s, %s,
                                    %s, %s,
                                    %s, %s,
                                    %s, %s, %s)
            ''', (text_title, text_author, author_id, text_language, text_original_publication_year,
                    text_original_publication_publisher, text_original_publication_publisher_loc,
                    text_original_publication_type, text_original_publication_length,
                    text_original_publication_length_type, text_writing_start, text_writing_end)
        )
    conn.close()

def insertEditions(data):
    conn = engine().connect()
    for i in data:
        edition_title = i['title']
        text_id = stringToInt(i["text_index"])
        edition_editor = stringToInt(i["additional_authors"])
        edition_publisher = stringToInt(i["publisher"])
        edition_publication_year = stringToInt(i["publication_year"])
        edition_isbn = stringToInt(i["isbn"])
        edition_isbn13 = stringToInt(i["isbn13"])
        edition_length = stringToInt(i["number_of_pages"])
        edition_language = stringToInt(i["language"])
        edition_binding = stringToInt(i["binding"])
        conn.execute('''INSERT INTO editions (edition_title, text_id, edition_editor, edition_publisher, edition_publication_year
                    , edition_isbn, edition_isbn13, edition_length, edition_language, edition_binding) 
                    VALUES (%s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s)
                    ''',(edition_title, text_id, edition_editor, edition_publisher, edition_publication_year
                        , edition_isbn, edition_isbn13, edition_length, edition_language, edition_binding)

        )
    conn.close()

with open ("database.json") as json_file: data = json.load(json_file)
#print(data["authors"][0:1])
#insertAuthors(data["authors"])
#insertTexts(data["texts"])
#insertEditions(data["editions"])