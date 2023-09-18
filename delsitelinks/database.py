from collections.abc import Generator
import logging
from os import remove
from os.path import expanduser
from typing import Any, Optional, Type, TypeVar

import mariadb
import sqlite3

from .config import QUERY_CHUNK_SIZE, DB_PATH, TOOLDB_NAME_FILE


LOG = logging.getLogger(__name__)
L = TypeVar('L', bound='LoggingDB')
R = TypeVar('R', bound='Replica')
T = TypeVar('T', bound='ToolDB')


class Replica:
    def __init__(self, dbname:str, dict_cursor:bool=True) -> None:
        params = {
            'host' : f'{dbname}.analytics.db.svc.wikimedia.cloud',
            'database' : f'{dbname}_p',
            'default_file' : f'{expanduser("~")}/replica.my.cnf'
        }
        self.connection = mariadb.connect(**params)
        self.cursor = self.connection.cursor(dictionary=dict_cursor)
        LOG.debug('replica database connection established')

    def __enter__(self):  #  -> mariadb.Cursor, mariadb.cursors.Cursor, mariadb._mariadb.Cursor
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()
        LOG.debug('replica database connection closed')

    @classmethod
    def query_mediawiki(cls:Type[R], dbname:str, query:str, params:Optional[dict[str, Any]]=None, params_tuple:Optional[tuple]=None) -> list[dict[str, Any]]:
        with cls(dbname) as db_cursor:
            try:
                if params is None and params_tuple is None:
                    db_cursor.execute(query)
                elif params is not None:
                    db_cursor.execute(query, params)
                elif params_tuple is not None:
                    db_cursor.execute(query, params_tuple)
            except mariadb.ProgrammingError as exception:
                msg = f'Failed to query "{query}" with params "{params}" at {dbname}'
                LOG.warn(msg)
                raise RuntimeError(msg) from exception
            result = db_cursor.fetchall()

        return result

    @classmethod
    def query_mediawiki_chunked(cls:Type[R], dbname:str, query:str, params:Optional[dict[str, Any]]=None, chunksize:int=QUERY_CHUNK_SIZE) -> Generator[list[dict[str, Any]], None, None]:
        with cls(dbname) as db_cursor:
            try:
                db_cursor.execute(query, params, buffered=False)
            except mariadb.ProgrammingError as exception:
                msg = f'Failed to query "{query}" with params "{params}" at {dbname}'
                LOG.warn(msg)
                raise RuntimeError(msg) from exception

            while True:
                try:
                    chunk = db_cursor.fetchmany(chunksize)
                except mariadb.InterfaceError as exception:
                    msg = 'Connection error during chunking'
                    LOG.warn(msg)
                    raise RuntimeError(msg) from exception
                if not len(chunk):  # check if cursor is empty
                    break
                yield chunk


class ToolDB:
    def __init__(self, autocommit:bool=False) -> None:
        params = {
            'host' : 'tools.db.svc.wikimedia.cloud',
            'database' : ToolDB._tooldb_name(),
            'default_file' : f'{expanduser("~")}/replica.my.cnf',
            'autocommit' : autocommit
        }
        try:
            self.connection = mariadb.connect(**params)
        except mariadb.ProgrammingError as exception:
            LOG.info('tool database connection could not be established; try to create tooldb')
            ToolDB._create_tooldb()
            self.connection = mariadb.connect(**params)

        self.cursor = self.connection.cursor(dictionary=True)
        self._create_tables()
        LOG.debug('tool database connection established')

    def __enter__(self):  #  -> tuple[mariadb.Connection, mariadb._mariadb.Cursor]
        return self.connection, self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()
        LOG.debug('tool database connection closed')

    def _create_tables(self) -> None:
        queries = [
            """CREATE TABLE IF NOT EXISTS pages (
                id INT(11) NOT NULL AUTO_INCREMENT,
                ns_numerical INT(11) NOT NULL,
                full_page_title VARBINARY(255) NOT NULL,
                qid VARBINARY(10) NOT NULL,
                PRIMARY KEY (id)
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""",
            """CREATE INDEX IF NOT EXISTS title ON pages (full_page_title)""",
            """CREATE TABLE IF NOT EXISTS sitelinks (
                id INT(11) NOT NULL AUTO_INCREMENT,
                sitelink VARBINARY(255) NOT NULL,
                qid_sitelink VARBINARY(10) NOT NULL,
                PRIMARY KEY (id)
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""",
            """CREATE INDEX IF NOT EXISTS title ON sitelinks (sitelink)"""
        ]

        for query in queries:
            self.cursor.execute(query)
            LOG.debug('created table for tool database')

    @classmethod
    def query_tooldb(cls:Type[T], query:str, params:Optional[dict[str, Any]]=None) -> list[dict[str, Any]]:
        with cls() as (_, db_cursor):
            try:
                if params is None:
                    db_cursor.execute(query)
                else:
                    db_cursor.execute(query, params)
            except mariadb.Error as exception:
                msg = f'Cannot make query "{query}" with params "{params}" against tool_db'
                LOG.warn(msg)
                raise RuntimeWarning(msg) from exception

            result = db_cursor.fetchall()

        return result

    @classmethod
    def clear_table(cls:Type[T], table:str) -> None:
        query = f'TRUNCATE TABLE {table}'

        with cls(autocommit=True) as (_, db_cursor):
            try:
                db_cursor.execute(query)
            except mariadb.Error as exception:
                msg = f'Cannot clear table {table}'
                LOG.error(msg)
                raise RuntimeWarning(msg) from exception

        LOG.info(f'Cleared table {table}')

    @classmethod
    def insert_batch(cls:Type[T], table:str, filename:str) -> None:
        column_mapper = {
            'pages' : ' (@id, ns_numerical, full_page_title, qid)',
            'sitelinks' : ' (@id, sitelink, qid_sitelink)',
        }

        query = f"""LOAD DATA LOCAL INFILE '{filename}'
        INTO TABLE {table}
        FIELDS TERMINATED BY '\t'
        LINES TERMINATED BY '\n'
        {column_mapper.get(table, '')}"""

        with cls() as (db_connection, db_cursor):
            try:
                db_cursor.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
                db_cursor.execute(query)
            except mariadb.Error as exception:
                msg = f'Cannot make import file {filename} into table {table} at tool_db'
                LOG.error(msg)
                raise RuntimeWarning(msg) from exception
            else:
                db_connection.commit()

        LOG.info(f'Inserted file into database table {table}')
        remove(filename)

    @staticmethod
    def _create_tooldb() -> None:
        db_params = {
            'host' : 'tools.db.svc.wikimedia.cloud',
            'default_file' : f'{expanduser("~")}/replica.my.cnf',
            'autocommit' : True
        }

        params = { 'tooldbname' : ToolDB._tooldb_name() }
        query = """CREATE DATABASE IF NOT EXISTS
            %(tooldbname)s
            DEFAULT CHARACTER SET utf8mb4
            COLLATE utf8mb4_bin"""

        # TODO: verify that this works with a prepared statement
        with mariadb.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

        LOG.info('tool database created')

    @staticmethod
    def _tooldb_name() -> str:
        with open(TOOLDB_NAME_FILE, mode='r', encoding='utf8') as file_handle:
            tooldb_name = file_handle.read()

        return tooldb_name.strip()


class LoggingDB:
    def __init__(self) -> None:
        self.connection = sqlite3.connect(DB_PATH)
        self.cursor = self.connection.cursor()

        self._create_tables()
        LOG.debug('logging database connetion established')

    def __enter__(self) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        return self.connection, self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()
        LOG.debug('logging database connection closed')

    def _create_tables(self) -> None:
        queries = [
            """CREATE TABLE IF NOT EXISTS
                sitelink_case (
                    qid TEXT,
                    dbname TEXT,
                    page_title TEXT,
                    revid INT
                )""",
            """CREATE TABLE IF NOT EXISTS
                sitelink_logevent (
                    sitelink_case_rowid INT,
                    logevent BLOB,
                    FOREIGN KEY (sitelink_case_rowid) REFERENCES sitelink_case (rowid)
                )""",
            """CREATE TABLE IF NOT EXISTS
                sitelink_logstr (
                    sitelink_case_rowid INT,
                    logstr BLOB,
                    FOREIGN KEY (sitelink_case_rowid) REFERENCES sitelink_case (rowid)
                )""",
            """CREATE TABLE IF NOT EXISTS
                sitelink_logparams (
                    sitelink_case_rowid INT,
                    p_key TEXT,
                    p_value BLOB,
                    p_dtype TEXT,
                    FOREIGN KEY (sitelink_case_rowid) REFERENCES sitelink_case (rowid)
                )""",
        ]

        for query in queries:
            with self.connection:
                self.connection.execute(query)
                LOG.debug('created table for logging database')

    # not in use right now
    @classmethod
    def query_logs(cls:Type[L], query:str, params:Optional[dict[str, Any]]=None) -> Optional[list[tuple]]:
        with cls() as (db_connection, db_cursor):
            if params is None:
                db_cursor.execute(query)
            else:
                db_cursor.execute(query, params)

            if not query.lower().startswith('select'):
                db_connection.commit()  # TODO: seems wrong; use transaction here

            if query.lower().startswith('select'):
                return db_cursor.fetchall()

        return None

    @classmethod
    def insert_log(cls:Type[L], revid:int, payload:dict[str, Any]) -> None:
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

        with cls() as (db_connection, db_cursor):
            db_cursor.execute(query_case, payload_case)
            rowid = db_cursor.lastrowid

            for key, value in payload.items():
                if key in [ 'qid', 'dbname', 'page_title' ]:
                    continue
                if key == 'log_event':
                    payload_event = {
                        'id' : rowid,
                        'logevent' : str(value)
                    }
                    db_cursor.execute(query_event, payload_event)                
                elif key == 'eval_str':
                    payload_str = {
                        'id' : rowid,
                        'logstr' : value
                    }
                    db_cursor.execute(query_str, payload_str)
                elif key == 'eval_params':
                    for key_params, value_params in value.items():
                        payload_params = {
                            'id' : rowid,
                            'key' : key_params,
                            'value' : str(value_params),
                            'datatype' : str(type(value_params))
                        }
                        db_cursor.execute(query_params, payload_params)

            db_connection.commit()
            LOG.info('inserted logging to database')
