from dotenv import load_dotenv
from TABLE_MODELS import Author, Text, Edition, engine
import json
from sqlalchemy.orm import sessionmaker


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
    session = Session()
    for i in data:
        name = transformString(i["name"])
        author = Author(
            author_name=name,
            author_positions = transformString(i["position"]),
            author_name_language="",
            author_birth_date=None,
            author_birth_city=None,
            author_birth_country=(i["country"]),
            author_birth_coordinates="",
            author_birth_year=stringToInt(i["birth"]),
            author_birth_month=None,
            author_birth_day=None,
            author_death_date=None,
            author_death_city=transformString(i["city_death"]),
            author_death_country=i["country_death"],
            author_death_coordinates="",
            author_death_year=stringToInt(i["death"]),
            author_death_month=None,
            author_death_day=None,
            author_nationality=transformString(i["nationality"]),
            author_gender=None,
            author_floruit=i["floruit"]
        )
        session.add(author)
    session.commit()
    session.close()

def insertTexts(data):
    session = Session()
    for i in data:
        text = Text(
            text_title=transformString(i["title"]),
            text_author=transformString(i["author"]),
            author_id=stringToInt(i["author_id"]),
            text_type=None,
            text_genre=None,
            text_language=stringToInt(i["language"]),
            text_original_publication_date=None,
            text_original_publication_year=stringToInt(i["publication"]),
            text_original_publication_month=None,
            text_original_publication_day=None,
            text_original_publication_publisher=stringToInt(i["publisher"]),
            text_original_publication_publisher_loc=stringToInt(i["publication_loc"]),
            text_original_publication_type=stringToInt(i["publication_type"]),
            text_original_publication_length=stringToInt(i["publication_length"]),
            text_original_publication_length_type=stringToInt(i["publication_length_type"]),
            text_writing_start=stringToInt(i["writing_start"]),
            text_writing_end=stringToInt(i["writing_end"])
        )
        session.add(text)
    session.commit()
    session.close()

def insertEditions(data):
    session = Session()
    for i in data:
        edition = Edition(
            edition_title=i['title'],
            author_id=None,
            text_id=stringToInt(i["text_index"]),
            edition_editor=stringToInt(i["additional_authors"]),
            edition_translator=None,
            edition_publisher=stringToInt(i["publisher"]),
            edition_publication_date=None,
            edition_publication_year=stringToInt(i["publication_year"]),
            edition_isbn=stringToInt(i["isbn"]),
            edition_isbn13=stringToInt(i["isbn13"]),
            edition_length=stringToInt(i["number_of_pages"]),
            edition_language=stringToInt(i["language"]),
            edition_binding=stringToInt(i["binding"])
        )
        session.add(edition)
    session.commit()
    session.close()

#with open ("database.json") as json_file: data = json.load(json_file)
#insertAuthors(data["authors"])
#insertTexts(data["texts"])
#insertEditions(data["editions"])