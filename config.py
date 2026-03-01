import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
BALLDONTLIE_API_KEY = os.getenv("BALLDONTLIE_API_KEY")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")

# Database
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME = os.getenv("DB_NAME", "llamaindex-dev")
DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
