from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

#Set up the connection to the database
def engine():
    load_dotenv()
    user = 'tasa99'
    password = os.getenv("pw")
    host = 'localhost'
    database = 'LECTURA'
    connection_string = f'postgresql://{user}:{password}@{host}/{database}'
    engine = create_engine(connection_string, connect_args={"options": "-c statement_timeout=200"})
    return engine

def read_sql(script):
    with open(script,"r") as file_content: return file_content.read()