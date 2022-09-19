from time import strftime
from concurrent.futures import ThreadPoolExecutor, as_completed
from mysql.connector import DatabaseError

from .config import NEEDS_FIX_WIKIS, WORK_WHITELIST, WORK_BLACKLIST, TOUCH_QID_DIFFERENT, TOUCH_QID_MISSING
from .helper import query_wiki_clients
from .processing import process_project
from .query import query_pages_to_file, query_sitelinks_to_file


def main_reload_dumps() -> None:
    wiki_clients = query_wiki_clients(dump_to_file=True)

    for i, wiki_client in enumerate(wiki_clients, start=1):
        if wiki_client.dbname in NEEDS_FIX_WIKIS:
            continue

        if WORK_BLACKLIST is not None and wiki_client.dbname in WORK_BLACKLIST:
            continue

        if WORK_WHITELIST is not None and wiki_client.dbname not in WORK_WHITELIST:
            continue

        print(f'\n== {wiki_client.dbname} ({i}/{len(wiki_clients)} at {strftime("%Y-%m-%d, %H:%M:%S")}) ==')
        with ThreadPoolExecutor() as executor:
            results = [
                executor.submit(query_pages_to_file, wiki_client),
                executor.submit(query_sitelinks_to_file, wiki_client)
            ]
            for future in as_completed(results):
                try:
                    filename, filesize = future.result()
                except DatabaseError as exception:
                    print(exception)
                else:
                    print(strftime("%Y-%m-%d, %H:%M:%S"), filename, f'{filesize/1024/1024:.2f}MiB')


# Remarks related to page touch:
# * arzwiki has still some work to do
# * azwiki has some problem with nulledits
# * cawikisource, ckbwiki, eswikiquote, eswikivoyage, simplewiki, zhwikinews have captchas to solve
# * specieswiki has some login problem
def main_tidy_sitelinks() -> None:
    wiki_clients = query_wiki_clients(dump_to_file=False, lazy_namespaces=True)

    for i, wiki_client in enumerate(wiki_clients, start=1):
        if wiki_client.dbname in NEEDS_FIX_WIKIS:
            continue

        if WORK_BLACKLIST is not None and wiki_client.dbname in WORK_BLACKLIST:
            continue

        if WORK_WHITELIST is not None and wiki_client.dbname not in WORK_WHITELIST:
            continue

        print(f'\n== {wiki_client.dbname} ({i}/{len(wiki_clients)}) at {strftime("%Y-%m-%d, %H:%M:%S")}) ==')
        process_project(wiki_client, job_remove_sitelinks=True, job_stat_file=False)


def main_power_touch() -> None:
    wiki_clients = query_wiki_clients(dump_to_file=True)

    with ThreadPoolExecutor() as executor:
        results = [ executor.submit(process_project, wiki_client, job_qid_different=TOUCH_QID_DIFFERENT, job_qid_missing=TOUCH_QID_MISSING, job_stat_file=False) for wiki_client in wiki_clients if (WORK_WHITELIST is not None and wiki_client.dbname in WORK_WHITELIST) and (WORK_BLACKLIST is not None and wiki_client.dbname not in WORK_BLACKLIST) ]
        for future in as_completed(results):
            success = future.result()
