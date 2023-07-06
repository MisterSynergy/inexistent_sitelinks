import logging
from time import strftime

import pywikibot as pwb

from .config import SPECIAL_PAGE_LOG


LOG = logging.getLogger(__name__)


def log_special_page_sitelink(qid:str, dbname:str, page_title:str) -> None:
    with open(SPECIAL_PAGE_LOG, mode='a', encoding='utf8') as file_handle:
        file_handle.write(f'{qid}\t{dbname}\t{page_title}\n')
    LOG.info(f'added sitelink {qid} --> {dbname} to special page log')


def clear_special_page_log() -> None:
    with open(SPECIAL_PAGE_LOG, mode='w', encoding='utf8') as file_handle:
        file_handle.write('')
    LOG.info('special page log was cleared')


def write_special_page_report() -> None:
    site = pwb.Site('wikidata', 'wikidata')
    page = pwb.Page(site, 'Wikidata:Database reports/Special pages as sitelinks')

    page_text = """List of sitelinks to MediaWiki Special pages. Update: <onlyinclude>{ts}</onlyinclude>

{{| class="wikitable sortable"
|-
! item !! project !! page title
{table_body}
|}}

[[Category:Database reports]]"""

    table_body = ''
    with open(SPECIAL_PAGE_LOG, mode='r', encoding='utf8') as file_handle:
        for line in file_handle:
            try:
                qid, dbname, page_title = line.strip().split('\t')
            except ValueError:
                break

            table_body = f'{table_body}|-\n| [[{qid}]] || {dbname} || {page_title}\n'

    page.text = page_text.format(
        ts=strftime('%Y-%m-%d %H:%M (%Z)'),
        table_body=table_body
    )
    page.save(summary='upd', minor=False)

    LOG.info('sucessfully wrote special page report')
