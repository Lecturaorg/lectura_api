from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os
import json
import pandas as pd

# Define the model for the table
Base = declarative_base()
class Author(Base):
    __tablename__ = 'AUTHORS'
    author_id = Column (Integer, primary_key=True)
    author_name = Column(String(255))
    author_positions = Column(String(255))
    author_name_language = Column(String(255))
    author_birth_date =  Column(String(255))
    author_birth_city = Column(String(255))
    author_birth_country = Column(String(255))
    author_birth_coordinates = Column(String(255))
    author_birth_year = Column(Integer)
    author_birth_month = Column(Integer)
    author_birth_day = Column(Integer)
    author_death_date = Column(String(255))
    author_death_city = Column(String(255))
    author_death_country = Column(String(255))
    author_death_coordinates = Column(String(255))
    author_death_year = Column(Integer)
    author_death_month = Column(Integer)
    author_death_day = Column(Integer)
    author_nationality = Column(String(255))
    author_gender = Column(String(255))
    author_floruit = Column(String(255))

class Text(Base):
    __tablename__ = "TEXTS"
    text_id=Column(Integer, primary_key = True)
    text_title=Column(String(255))
    text_author=Column(String(255))
    author_id=Column(Integer)
    text_type=Column(String(255))
    text_genre=Column(String(255))
    text_language=Column(String(255))
    text_original_publication_date=Column(String(255))
    text_original_publication_year=Column(Integer)
    text_original_publication_month=Column(Integer)
    text_original_publication_day=Column(Integer)
    text_original_publication_publisher=Column(String(255))
    text_original_publication_publisher_loc=Column(String(255))
    text_original_publication_type=Column(String(255))
    text_original_publication_length=Column(Integer)
    text_original_publication_length_type=Column(String(255))
    text_writing_start=Column(Integer)
    text_writing_end=Column(Integer)

class Edition(Base):
    __tablename__ = "EDITIONS"
    edition_id=Column(Integer, primary_key = True)
    edition_title=Column(String(255))
    author_id=Column(Integer)
    text_id=Column(Integer)
    edition_editor=Column(String(255))
    edition_translator=Column(String(255))
    edition_publisher=Column(String(255))
    edition_publication_date=Column(DateTime)
    edition_publication_year=Column(Integer)
    edition_isbn=Column(String(255))
    edition_isbn13=Column(String(255))
    edition_length=Column(Integer)
    edition_language=Column(String(255))
    edition_binding=Column(String(255))


#Set up the connection to the database
def engine():
    load_dotenv()
    user = 'tasa99'
    password = os.getenv("pw")
    host = 'localhost'
    database = 'LECTURA'
    connection_string = f'postgresql://{user}:{password}@{host}/{database}'
    engine = create_engine(connection_string, connect_args={"options": "-c statement_timeout=100"})
    return engine

