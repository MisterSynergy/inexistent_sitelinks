import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import NEEDS_FIX_WIKIS, WORK_WHITELIST, WORK_BLACKLIST, MIN_PROJECT, MAX_PROJECT, TOUCH_QID_DIFFERENT, TOUCH_QID_MISSING
from .database import Replica
from .types import WikiClient
from .query_replicas import query_pages, query_sitelinks
from .query_tooldb import query_missing_page_df, query_local_qid_is_different_df, query_local_qid_is_missing_df
from .processing_sitelinks import remove_sitelinks
from .processing_touch import touch_different_local_qids, touch_missing_local_qids


LOG = logging.getLogger(__name__)


def process_project(wiki_client:WikiClient, job_remove_sitelinks:bool=False, job_qid_different:bool=False, job_qid_missing:bool=False) -> None:  # TODO: default input args
    # threading does not speed up things here since both methods operate on the same database and the
    # operation is apparently limited by database-io anyways. however, this way both queries are started
    # at the same time and thus keeping them synced    
    try:
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(query_pages, wiki_client),
                executor.submit(query_sitelinks, wiki_client)
            ]
            for future in as_completed(futures):
                future.result()
    except RuntimeError as exception:  # this catches particularly lost database connection situations
        LOG.warn(exception)
        return

    if job_remove_sitelinks is True:
        page_is_missing = query_missing_page_df()
        #page_is_missing.to_csv(f'./{wiki_client.dbname}-page_is_missing.tsv', sep='\t', header=False)

        remove_sitelinks(page_is_missing, wiki_client)

    if job_qid_different is True:
        local_qid_is_different = query_local_qid_is_different_df()
        #local_qid_is_different.to_csv(f'./{wiki_client.dbname}-local_qid_is_different.tsv', sep='\t', header=False)

        touch_different_local_qids(local_qid_is_different, wiki_client)

    if job_qid_missing is True:
        local_qid_is_missing = query_local_qid_is_missing_df()
        #local_qid_is_missing.to_csv(f'./{wiki_client.dbname}-local_qid_is_missing.tsv', sep='\t', header=False)

        touch_missing_local_qids(local_qid_is_missing, wiki_client)


def query_wiki_clients(lazy_namespaces:bool=False) -> list[WikiClient]:
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
    for dct in result:
        dbname = dct['dbname']
        hostname = dct['url'][8:]

        wiki_clients.append(WikiClient(dbname, hostname, lazy_namespaces=lazy_namespaces))

    return wiki_clients


def main_tidy_sitelinks() -> None:
    wiki_clients = query_wiki_clients(lazy_namespaces=True)

    for i, wiki_client in enumerate(wiki_clients, start=1):
        if wiki_client.dbname in NEEDS_FIX_WIKIS:
            continue

        if WORK_BLACKLIST is not None and wiki_client.dbname in WORK_BLACKLIST:
            continue

        if WORK_WHITELIST is not None and wiki_client.dbname not in WORK_WHITELIST:
            continue

        if MIN_PROJECT is not None and wiki_client.dbname < MIN_PROJECT:
            continue

        if MAX_PROJECT is not None and wiki_client.dbname > MAX_PROJECT:
            continue

        LOG.info(f'{wiki_client.dbname} ({i}/{len(wiki_clients)})')
        process_project(wiki_client, job_remove_sitelinks=True, job_qid_different=False, job_qid_missing=False)


# Remarks related to page touch:
# * arzwiki has still some work to do
# * azwiki has some problem with nulledits
# * cawikisource, ckbwiki, eswikiquote, eswikivoyage, simplewiki, zhwikinews have captchas to solve
# * specieswiki has some login problem
def main_power_touch() -> None:
    wiki_clients = query_wiki_clients(lazy_namespaces=True)

    for i, wiki_client in enumerate(wiki_clients, start=1):
        if wiki_client.dbname in NEEDS_FIX_WIKIS:
            continue

        if WORK_BLACKLIST is not None and wiki_client.dbname in WORK_BLACKLIST:
            continue

        if WORK_WHITELIST is not None and wiki_client.dbname not in WORK_WHITELIST:
            continue

        if MIN_PROJECT is not None and wiki_client.dbname < MIN_PROJECT:
            continue

        if MAX_PROJECT is not None and wiki_client.dbname > MAX_PROJECT:
            continue
        
        LOG.info(f'{wiki_client.dbname} ({i}/{len(wiki_clients)})')
        process_project(wiki_client, job_qid_different=True, job_qid_missing=True)
