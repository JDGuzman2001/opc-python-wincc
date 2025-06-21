import os
from dotenv import load_dotenv
from enum import Enum


# Load environment variables
load_dotenv()

class APIConfig:
    # Google Cloud credentials
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    PROJECT_ID = os.getenv("PROJECT_ID")

    if not GOOGLE_APPLICATION_CREDENTIALS or not PROJECT_ID:
        raise ValueError("Missing Google Cloud credentials")
    
    # Google Cloud configuration
    SQL_DATABASE_NAME = os.getenv("SQL_DATABASE_NAME")
    SQL_DATABASE_USERNAME = os.getenv("SQL_DATABASE_USERNAME")
    SQL_DATABASE_PASSWORD = os.getenv("SQL_DATABASE_PASSWORD")
    SQL_DATABASE_REGION = os.getenv("SQL_DATABASE_REGION")
    SQL_DATABASE_INSTANCE = os.getenv("SQL_DATABASE_INSTANCE")

    if not SQL_DATABASE_NAME or not SQL_DATABASE_USERNAME or not SQL_DATABASE_PASSWORD or not SQL_DATABASE_REGION or not SQL_DATABASE_INSTANCE:
        raise ValueError("Missing Google Cloud configuration")
    
    # OpenAI API key
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    if not OPENAI_API_KEY:
        raise ValueError("Missing OpenAI API key")

config = APIConfig()
    
    
    

