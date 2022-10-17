import logging

import pandas as pd

from .types import WikiClient
from .bot_touch import touch_page


LOG = logging.getLogger(__name__)


def _touch_pages(df:pd.DataFrame, wiki_client:WikiClient) -> None:
    for i, elem in enumerate(df.itertuples(), start=1):
        try:
            touch_page(
                wiki_client.dbname,
                elem.sitelink
            )
        except RuntimeWarning:
            LOG.warn(f'did not touch "{elem.sitelink}" on project' \
                     f' {wiki_client.dbname}')
        else:
            if i%100==0:
                LOG.info(f'touched "{elem.sitelink}" on project' \
                         f' {wiki_client.dbname} ({i}/{df.shape[0]})')


def touch_different_local_qids(df:pd.DataFrame, wiki_client:WikiClient) -> None:
    LOG.info(f'Page needs nulledit in {wiki_client.dbname} because the local' \
             f' qid in the page_props table is different: {df.shape[0]}')
    _touch_pages(df, wiki_client)


def touch_missing_local_qids(df:pd.DataFrame, wiki_client:WikiClient) -> None:
    LOG.info(f'Pages need nulledit in {wiki_client.dbname} because page_props' \
             f' value is missing: {df.shape[0]}')
    _touch_pages(df, wiki_client)
