from os.path import getsize

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow.ipc import IpcWriteOptions, new_file

from .config import PAGES_FILENAME, SITELINKS_FILENAME, IX_URL, PAGE_IS_MISSING, LOCAL_QID_IS_DIFFERENT, LOCAL_QID_IS_MISSING
from .database import Replica
from .types import WikiClient


def query_pages_to_feather(wiki_client:WikiClient, query:str) -> tuple[str, int]:
    filename = PAGES_FILENAME.format(dbname=wiki_client.dbname)
    
    schema = pa.schema(
        [
            pa.field('ns_numerical', pa.int32()),
            pa.field('full_page_title', pa.string()),
            pa.field('qid', pa.string())
        ]
    )

    with pa.OSFile(filename, 'wb') as sink:
        with new_file(sink, schema, options=IpcWriteOptions(compression='lz4')) as writer:
            for result in Replica.query_mediawiki_chunked(wiki_client.dbname, query):
                page_namespaces = [ row[0] for row in result ]
                page_titles = [ row[1].decode('utf8').replace('_', ' ') for row in result ]
                full_page_titles = [ page_title if ns==0 else f'{wiki_client.get_namespace_by_id(ns)}:{page_title}' for (ns, page_title) in zip(page_namespaces, page_titles) ]
                local_qids = [ row[2].decode('utf8') if row[2] is not None else 'None' for row in result ]
                local_qid_is_null = [ True if local_qid=='None' else False for local_qid in local_qids ]
                
                batch = pa.record_batch(
                    [
                        pa.array(page_namespaces, type=pa.int32()),
                        pa.array(full_page_titles, type=pa.string()),
                        pa.array(local_qids, type=pa.string(), mask=np.array(local_qid_is_null, dtype=bool))
                    ],
                    schema
                )
                writer.write(batch)

    return filename, getsize(filename)


def query_pages_to_file(wiki_client:WikiClient) -> tuple[str, int]:
    query_constraints = {
        'wikidatawiki' : ' WHERE page_namespace!=0',
        'commonswiki' : ' WHERE page_namespace!=6'
    }

    query = f"""SELECT
        page_namespace,
        page_title,
        pp_value,
        page_id
    FROM
        page
            LEFT JOIN page_props
                ON page_id=pp_page
                AND pp_propname='wikibase_item'{query_constraints.get(wiki_client.dbname, '')}"""

    return query_pages_to_feather(wiki_client, query)


def query_sitelinks_to_feather(wiki_client:WikiClient, query:str) -> tuple[str, int]:
    filename = SITELINKS_FILENAME.format(dbname=wiki_client.dbname)
    
    schema = pa.schema(
        [
            pa.field('qid_sitelink', pa.string()),
            pa.field('sitelink', pa.string())
        ]
    )

    with pa.OSFile(filename, 'wb') as sink:
        with new_file(sink, schema, options=IpcWriteOptions(compression='lz4')) as writer:
            for result in Replica.query_mediawiki_chunked('wikidatawiki', query):
                qid_sitelinks = [ row[0] for row in result ]
                ips_site_pages = [ row[1].decode('utf8') for row in result ]

                batch = pa.record_batch(
                    [
                        pa.array(qid_sitelinks, type=pa.string()),
                        pa.array(ips_site_pages, type=pa.string())
                    ],
                    schema
                )
                writer.write(batch)

    return filename, getsize(filename)


def query_sitelinks_to_file(wiki_client:WikiClient) -> tuple[str, int]:
    query = f"""SELECT
        CONCAT('Q', ips_item_id) AS qid_sitelink,
        ips_site_page
    FROM
        wb_items_per_site
    WHERE
        ips_site_id='{wiki_client.dbname}'"""

    return query_sitelinks_to_feather(wiki_client, query)


def load_file_from_ix(folder:str, filename:str) -> pd.DataFrame:
    if folder not in [ PAGE_IS_MISSING, LOCAL_QID_IS_DIFFERENT, LOCAL_QID_IS_MISSING ]:
        raise RuntimeWarning(f'load_file_from_ix: target folder {folder} does not exist')

    df = pd.read_feather(IX_URL.format(folder=folder, filename=filename))

    return df
