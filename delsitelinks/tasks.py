from time import strftime
from concurrent.futures import ThreadPoolExecutor, as_completed
from mysql.connector import DatabaseError

from .config import NEEDS_FIX_WIKIS
from .helper import query_wiki_clients
from .processing import process_project
from .query import query_pages_to_file, query_sitelinks_to_file


def main_reload_dumps() -> None:
    wiki_clients = query_wiki_clients(dump_to_file=True)

    for i, wiki_client in enumerate(wiki_clients, start=1):
        print(f'\n== {wiki_client.dbname} ({i}/{len(wiki_clients)} at {strftime("%Y-%m-%d, %H:%M:%S")}) ==')

        if wiki_client.dbname in NEEDS_FIX_WIKIS:
            print('Skip: issue to be solved later')
            continue

        if wiki_client.dbname != 'wuuwiki':
            continue

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


def main_tidy_sitelinks() -> None:
    wiki_clients = query_wiki_clients(dump_to_file=False, lazy_namespaces=True)

    for i, wiki_client in enumerate(wiki_clients, start=1):
        #if wiki_client.dbname not in [ 'fawiki', 'ruwiki', 'hewiki', 'arwiki', 'ptwiki', 'cawiki', 'zhwiki', 'enwiki', 'metawiki', 'kowiki', 'zhwiktionary', 'zhwikibooks', 'skwiki', 'roa_rupwiki', 'plwiki', 'mywiki', 'guwiki', 'kawiki', 'azbwiki', 'fiwiki', 'fawikivoyage', 'eswiktionary', 'elwikisource', 'diqwiki', 'dewikivoyage', 'dewiki', 'dawiktionary', 'bewikisource' ]:
        #    continue
        if wiki_client.dbname not in [ 'wuuwiki' ]:
            continue

        if wiki_client.dbname in NEEDS_FIX_WIKIS:
            print('Skip: issue to be solved later')
            continue

        # arzwiki has still some work to do
        # azwiki has some problem with nulledits
        # cawikisource, ckbwiki, eswikiquote, eswikivoyage, simplewiki, zhwikinews have captchas to solve
        # specieswiki has some login problem

        print(f'\n== {wiki_client.dbname} ({i}/{len(wiki_clients)}) ==')

        process_project(wiki_client, job_remove_sitelinks=True, job_stat_file=False)


def main_power_touch() -> None:
    work_dbnames = [ 'viwiki' ]

    wiki_clients = query_wiki_clients(dump_to_file=True)

    with ThreadPoolExecutor() as executor:
        results = [ executor.submit(process_project, wiki_client, job_qid_different=True, job_qid_missing=False, job_stat_file=False) for wiki_client in wiki_clients if wiki_client.dbname in work_dbnames ]
        for future in as_completed(results):
            success = future.result()
