import logging
import pandas as pd

from .database import Replica, ToolDB
from .types import WikiClient


TOOLDB_TMP_PAGES_FILE:str = './tmp_tooldb_pages.tsv'
TOOLDB_TMP_SITELINKS_FILE:str = './tmp_tooldb_sitelinks.tsv'
LOG = logging.getLogger(__name__)


def query_pages(wiki_client:WikiClient) -> None:
    query_constraints = {
        'wikidatawiki' : ' WHERE page_namespace!=0',
        'commonswiki' : ' WHERE page_namespace!=6'
    }

    query = f"""SELECT
        page_namespace AS ns_numerical,
        CONVERT(page_title USING utf8mb4) AS page_title,
        CONVERT(pp_value USING utf8mb4) AS qid
    FROM
        page
            LEFT JOIN page_props
                ON page_id=pp_page
                AND pp_propname='wikibase_item'{query_constraints.get(wiki_client.dbname, '')}"""

    ToolDB.clear_table('pages')

    for chunk in Replica.query_mediawiki_chunked(wiki_client.dbname, query):
        df = pd.DataFrame(data=chunk)
        
        df['page_title'] = df['page_title'].str.replace('_', ' ')
        df['ns_lexical_with_colon'] = ''
        for ns in df['ns_numerical'].unique():
            if ns == 0:
                continue
            df.loc[df['ns_numerical']==ns, 'ns_lexical_with_colon'] = f'{wiki_client.get_namespace_by_id(ns)}:'
        df['full_page_title'] = df['ns_lexical_with_colon'] + df['page_title']
        df.drop(columns=['page_title', 'ns_lexical_with_colon'], inplace=True)
        
        df.to_csv(
            TOOLDB_TMP_PAGES_FILE,
            sep='\t',
            header=False,
            columns=['ns_numerical', 'full_page_title', 'qid']
        )

        ToolDB.insert_batch('pages', TOOLDB_TMP_PAGES_FILE)


def query_sitelinks(wiki_client:WikiClient) -> None:
    params = { 'dbname' : wiki_client.dbname }
    query = f"""SELECT
        CONVERT(ips_site_page USING utf8mb4) AS sitelink,
        CONCAT('Q', ips_item_id) AS qid_sitelink
    FROM
        wb_items_per_site
    WHERE
        ips_site_id=%(dbname)s"""

    ToolDB.clear_table('sitelinks')

    for chunk in Replica.query_mediawiki_chunked('wikidatawiki', query, params=params):
        df = pd.DataFrame(data=chunk)

        df.to_csv(
            TOOLDB_TMP_SITELINKS_FILE,
            sep='\t',
            header=False,
            columns=['sitelink', 'qid_sitelink']
        )

        ToolDB.insert_batch('sitelinks', TOOLDB_TMP_SITELINKS_FILE)
