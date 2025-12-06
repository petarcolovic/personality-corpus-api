import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Učitamo .env fajl
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "korpus")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


def get_connection():
    """
    Otvara novu konekciju ka bazi.
    Poziva se iz context managera 'get_db'.
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    return conn


@contextmanager
def get_db():
    """
    Koristi se kao:

        with get_db() as conn:
            ...

    i automatski zatvara konekciju.
    """
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
