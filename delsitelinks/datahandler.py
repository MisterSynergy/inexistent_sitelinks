from os.path import isfile
from typing import Any
from csv import QUOTE_NONE
import pandas as pd

from .config import RELOAD, PAGES_FILENAME, SITELINKS_FILENAME, \
    PAGE_IS_MISSING_FILENAME, LOCAL_QID_IS_DIFFERENT_FILENAME, LOCAL_QID_IS_MISSING_FILENAME
from .query import query_pages_to_file, query_sitelinks_to_file
from .types import WikiClient


def load_pages(wiki_client:WikiClient) -> pd.DataFrame:
    filename = PAGES_FILENAME.format(dbname=wiki_client.dbname)

    if RELOAD is True or isfile(filename) is not True:
        query_pages_to_file(wiki_client)

    df = pd.read_feather(filename)

    return df


def load_sitelinks(wiki_client:WikiClient) -> pd.DataFrame:
    filename = SITELINKS_FILENAME.format(dbname=wiki_client.dbname)

    if RELOAD is True or isfile(filename) is not True:
        query_sitelinks_to_file(wiki_client)

    df = pd.read_feather(filename)

    return df


def _load_dump_feather(filename:str) -> pd.DataFrame:
    if not isfile(filename):
        raise RuntimeWarning('no file to load found')

    return pd.read_feather(filename)


def load_page_is_missing(wiki_client:WikiClient) -> pd.DataFrame:
    df = _load_dump_feather(PAGE_IS_MISSING_FILENAME.format(dbname=wiki_client.dbname))

    return df


def load_local_qid_is_different(wiki_client:WikiClient) -> pd.DataFrame:
    df = _load_dump_feather(LOCAL_QID_IS_DIFFERENT_FILENAME.format(dbname=wiki_client.dbname))

    return df


def load_local_qid_is_missing(wiki_client:WikiClient) -> pd.DataFrame:
    df = _load_dump_feather(LOCAL_QID_IS_MISSING_FILENAME.format(dbname=wiki_client.dbname))

    return df
