import json
import logging
from spotipy.cache_handler import CacheHandler
import psycopg2 as pg

logger = logging.getLogger(__name__)

class PostgresCacheHandler(CacheHandler):
    """
    A cache handler that stores the token info in the Postgres db.
    """

    def __init__(self, conn, key=None):
        """
        Parameters:
            * conn: psycopg2 connection
            * key: May be supplied, will otherwise be generated
                   (takes precedence over `token_info`)
        """
        self.conn = conn
        self.key = key if key else 'token_info'

    def get_cached_token(self):
        token_info = None
        try:
            cur = self.conn.cursor()
            cur.execute(f"SELECT value FROM spotipy_cache WHERE key = '{self.key}';")
            token_info = cur.fetchone()
            if token_info:
                return json.loads(token_info[0])
        except pg.Error as e:
            logger.warning('Error getting token from cache: ' + str(e))

        return token_info

    def save_token_to_cache(self, token_info):
        try:
            cur = self.conn.cursor()
            cur.execute(f"""INSERT INTO spotipy_cache (key, value)
                            VALUES ('{self.key}', '{json.dumps(token_info)}')
                            ON CONFLICT (key) DO UPDATE
                            SET value = EXCLUDED.value;""")
            self.conn.commit()
        except pg.Error as e:
            logger.warning('Error saving the token to cache: ' + str(e))

