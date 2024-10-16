from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Database untuk Amazon Sales
SOURCE_DB_USERNAME = os.getenv("SOURCE_DB_USERNAME")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD")
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT", "5432")  
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME")

# Database untuk Load
LOAD_DB_USERNAME = os.getenv("LOAD_DB_USERNAME")
LOAD_DB_PASSWORD = os.getenv("LOAD_DB_PASSWORD")
LOAD_DB_HOST = os.getenv("LOAD_DB_HOST")
LOAD_DB_PORT = os.getenv("LOAD_DB_PORT", "5433")  
LOAD_DB_NAME = os.getenv("LOAD_DB_NAME")

def postgres_amazon_engine():
    """
    Function yang digunakan untuk membuat engine postgres
    yang tujuannya untuk fetch data dari amazon database.
    Sesuaikan username, password, host, dan nama database dengan milik masing - masing

    """
    engine = create_engine(f"postgresql://{SOURCE_DB_USERNAME}:{SOURCE_DB_PASSWORD}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}")
    return engine

def postgres_load_engine():
    """
    Function yang digunakan untuk membuat engine postgres
    yang tujuannya untuk load data ke recommender system database.
    Sesuaikan username, password, host, dan nama database dengan milik masing - masing
    """
    load_engine = create_engine(f"postgresql://{LOAD_DB_USERNAME}:{LOAD_DB_PASSWORD}@{LOAD_DB_HOST}:{LOAD_DB_PORT}/{LOAD_DB_NAME}")
    return load_engine
