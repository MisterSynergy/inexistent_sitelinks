import pandas as pd

from .config import PAGE_IS_MISSING, LOCAL_QID_IS_DIFFERENT, LOCAL_QID_IS_MISSING
from .database import Replica
from .types import WikiClient


def query_wiki_clients(dump_to_file:bool=False, lazy_namespaces:bool=False) -> list[WikiClient]:
    query = """SELECT
        dbname,
        url
    FROM
        wiki
    WHERE
        is_closed=0
        AND has_wikidata=1
    ORDER BY
        dbname ASC"""  # https://quarry.wmcloud.org/query/12744
    result = Replica.query_mediawiki('meta', query)

    wiki_clients = []
    for tpl in result:
        dbname = tpl[0]
        hostname = tpl[1][8:]

        wiki_clients.append(WikiClient(dbname, hostname, lazy_namespaces=lazy_namespaces))

    write_to_wikiclients_file(wiki_clients, dump_to_file)

    return wiki_clients


def get_missing_filter(df:pd.DataFrame, key:str) -> pd.Series:
    if key == PAGE_IS_MISSING:
        return df['ns_numerical'].isna()
    elif key == LOCAL_QID_IS_DIFFERENT:
        return (df['qid'].notna() & (df['qid']!=df['qid_sitelink']))
    elif key == LOCAL_QID_IS_MISSING:
        return (df['qid'].isna() & df['ns_numerical'].notna())

    raise RuntimeWarning(f'No filter found for key {key}')


def write_to_stat_file(dbname:str, params:dict) -> None:
    with open('./log/stat.tsv', mode='a', encoding='utf8') as file_handle:
        file_handle.write(f'{dbname}\t{params.get(PAGE_IS_MISSING, -1):d}\t{params.get(LOCAL_QID_IS_DIFFERENT, -1):d}\t{params.get(LOCAL_QID_IS_MISSING, -1):d}\n')


def write_to_wikiclients_file(wiki_clients:list[WikiClient], dump_to_file:bool=False) -> None:
    if dump_to_file is not True:
        return

    with open('./log/wiki_clients.txt', mode='w', encoding='utf8') as file_handle:
        for wiki_client in wiki_clients:
            file_handle.write(f'{wiki_client.dbname}\t{wiki_client.hostname}\n')
