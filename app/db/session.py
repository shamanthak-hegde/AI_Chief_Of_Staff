import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.config import get_settings


settings = get_settings()


def get_conn():
    return psycopg2.connect(settings.database_url)


def get_cursor():
    conn = get_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
