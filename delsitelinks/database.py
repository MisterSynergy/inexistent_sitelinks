from os.path import expanduser, exists
from typing import Any, Optional, Iterator
import sqlite3

from mysql.connector import MySQLConnection
from mysql.connector.errors import ProgrammingError

from .config import QUERY_CHUNK_SIZE, DB_PATH


class Replica:
    def __init__(self, dbname:str) -> None:
        params = {
            'host' : f'{dbname}.analytics.db.svc.wikimedia.cloud',
            'database' : f'{dbname}_p',
            'option_files' : f'{expanduser("~")}/.my.cnf' # user credentials from PAWS home folder
        }
        self.replica = MySQLConnection(**params)
        self.cursor = self.replica.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()

    @staticmethod
    def query_mediawiki(dbname:str, query:str) -> list[tuple]:
        with Replica(dbname) as db_cursor:
            try:
                db_cursor.execute(query)
            except ProgrammingError as exception:  # TODO
                print(exception)
                print(dbname)
                print(query)
                return []
            result = db_cursor.fetchall()

        return result

    @staticmethod
    def query_mediawiki_chunked(dbname:str, query:str, chunksize:int=QUERY_CHUNK_SIZE) -> Iterator[list[tuple]]:
        with Replica(dbname) as db_cursor:
            db_cursor.execute(query)
            while True:
                chunk = db_cursor.fetchmany(chunksize)
                if not len(chunk):  # check if cursor is empty
                    break
                yield chunk


class LoggingDB:
    def __init__(self) -> None:
        self.connection = sqlite3.connect(DB_PATH)
        self.cursor = self.connection.cursor()
        
        self._create_tables()

    def _create_tables(self) -> None:
        queries = [
            """CREATE TABLE IF NOT EXISTS sitelink_case (qid TEXT, dbname TEXT, page_title TEXT, revid INT)""",
            """CREATE TABLE IF NOT EXISTS sitelink_logevent (sitelink_case_rowid INT, logevent BLOB, FOREIGN KEY (sitelink_case_rowid) REFERENCES sitelink_case (rowid))""",
            """CREATE TABLE IF NOT EXISTS sitelink_logstr (sitelink_case_rowid INT, logstr BLOB, FOREIGN KEY (sitelink_case_rowid) REFERENCES sitelink_case (rowid))""",
            """CREATE TABLE IF NOT EXISTS sitelink_logparams (sitelink_case_rowid INT, p_key TEXT, p_value BLOB, p_dtype TEXT, FOREIGN KEY (sitelink_case_rowid) REFERENCES sitelink_case (rowid))""",
        ]

        for query in queries:
            with self.connection:
                self.connection.execute(query)

    def __enter__(self):
        return self.connection, self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

    @staticmethod
    def query_logs(query:str, params:Optional[dict[str, Any]]=None) -> Optional[list[tuple]]:
        with LoggingDB() as (db_connection, db_cursor):
            if params is None:  # plain query in string form
                db_cursor.execute(query)
            else:  # query consisting of a prepared statement and parameters
                db_cursor.execute(query, params)

            if not query.lower().startswith('select'):
                db_connection.commit()
            
            if query.lower().startswith('select'):
                return db_cursor.fetchall()

        return None

    @staticmethod
    def insert_log(revid:int, payload:dict[str, Any]) -> None:
        query_case = """INSERT INTO sitelink_case VALUES (:qid, :dbname, :page_title, :revid)"""
        payload_case = {
            'qid' : payload.get('qid', ''),
            'dbname' : payload.get('dbname', ''),
            'page_title' : payload.get('page_title', ''),
            'revid' : revid
        }

        query_event = """INSERT INTO sitelink_logevent VALUES (:id, :logevent)"""
        query_str = """INSERT INTO sitelink_logstr VALUES (:id, :logstr)"""
        query_params = """INSERT INTO sitelink_logparams VALUES (:id, :key, :value, :datatype)"""

        with LoggingDB() as (db_connection, db_cursor):
            db_cursor.execute(query_case, payload_case)
            rowid = db_cursor.lastrowid

            for key, value in payload.items():
                if key in [ 'qid', 'dbname', 'page_title' ]:
                    continue
                if key == 'log_event':
                    payload_event = { 'id' : rowid, 'logevent' : str(value) }
                    db_cursor.execute(query_event, payload_event)                
                elif key == 'eval_str':
                    payload_str = { 'id' : rowid, 'logstr' : value }
                    db_cursor.execute(query_str, payload_str)
                elif key == 'eval_params':
                    for key_params, value_params in value.items():
                        payload_params = { 'id' : rowid, 'key' : key_params, 'value' : str(value_params), 'datatype' : str(type(value_params)) }
                        db_cursor.execute(query_params, payload_params)

            db_connection.commit()
