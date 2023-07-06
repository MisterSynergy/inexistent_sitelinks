from datetime import datetime
import logging
from time import strftime
from typing import Any, Optional

import pandas as pd

from .config import QIDS_TO_IGNORE, MAX_SITELINKS_PER_PROJECT
from .types import WikiClient, Page, Sitelink, LogEvent
from .bot_sitelinks import remove_sitelink_from_item, canonicalize_sitelink, normalize_title, \
    check_if_item_has_sitelink, check_if_page_exists_on_client, check_if_page_is_redirect
from .special_pages_report import log_special_page_sitelink


LOG = logging.getLogger(__name__)


def _make_callback_payload(qid:str, dbname:str, page_title:str, log_event:Optional[dict[str, Any]]=None, \
                           eval_params:Optional[dict]=None, eval_str:Optional[str]=None) -> dict[str, Any]:
    callback_payload:dict[str, Any] = {
        'qid' : qid,
        'dbname' : dbname,
        'page_title' : page_title
    }
    if log_event is not None:
        callback_payload['log_event'] = log_event
    if eval_params is not None:
        callback_payload['eval_params'] = eval_params
    if eval_str is not None:
        callback_payload['eval_str'] = eval_str
    
    return callback_payload


def remove_sitelinks(df:pd.DataFrame, wiki_client:WikiClient) -> None:
    LOG.info(f'Cases of sitelinks to inexistent pages in {wiki_client.dbname}: {df.shape[0]}')

    if df.shape[0] > MAX_SITELINKS_PER_PROJECT:
        df = df.sample(MAX_SITELINKS_PER_PROJECT)
    
    for elem in df.itertuples():
        page = Page(
            elem.sitelink,
            wiki_client,
            page_namespace=elem.ns_numerical,
            qid_local=elem.qid
        )

        sitelink = Sitelink(
            elem.qid_sitelink,
            wiki_client,
            page
        )

        process_sitelink(sitelink)


def process_sitelink(sitelink:Sitelink) -> None:  # TODO: tidy
    try:
        item_has_sitelink = check_if_item_has_sitelink(sitelink.qid, sitelink.wiki_client.dbname, sitelink.page.page_title)
    except RuntimeWarning:
        return
    if not item_has_sitelink:
        if sitelink.page.page_title in sitelink.page.alternative_page_titles:
            process_sitelink_alt_title(sitelink)
            return
        else:
            LOG.info(f'Item {sitelink.qid} does not have a sitelink "{sitelink.page.page_title}" for {sitelink.wiki_client.dbname}')
            return # nothing to do

    try:
        page_exists = check_if_page_exists_on_client(sitelink.wiki_client.dbname, sitelink.page.page_title)
    except RuntimeWarning:
        log_special_page_sitelink(sitelink.qid, sitelink.wiki_client.dbname, sitelink.page.page_title)
        page_exists = False

    if sitelink.qid in QIDS_TO_IGNORE:
        return

    if page_exists:
        process_sitelink_title_normalization(sitelink)
        return

    if not len(sitelink.page.log_events):
        process_sitelink_no_logevent(sitelink)
        return

    log_event = sitelink.page.lastest_log_event
    if log_event is None:
        return

    eval_str = []
    eval_str.append(f'{sitelink.qid}, {sitelink.wiki_client.dbname}, {sitelink.page.page_title}, {log_event}')
    eval_str.append(str(log_event.user))

    eval_params = log_event.user.get_bot_payload_dict()

    if log_event.log_action in [ 'move', 'move_redir' ]:
        process_sitelink_move(sitelink, log_event, eval_str, eval_params)
    elif log_event.log_action == 'delete':
        process_sitelink_delete(sitelink, log_event, eval_str, eval_params)


def process_sitelink_no_logevent(sitelink:Sitelink) -> None:  # TODO: does nothing
    eval_str = f'Cannot find a log timestamp for page "{sitelink.page.page_title}" on {sitelink.wiki_client.dbname} in {sitelink.qid}'
    LOG.debug(eval_str)


def process_sitelink_alt_title(sitelink:Sitelink) -> None:
    eval_str = f'Item {sitelink.qid} has sitelink "{sitelink.page.page_title}" with namespace alias' \
               f' for {sitelink.wiki_client.dbname}; so normalize to "{sitelink.page.canonical_page_title}"'
    eval_params = {
        'sitelink_with_alt_title' : True,
        'alt_title' : sitelink.page.page_title,
        'canonical_title' : sitelink.page.canonical_page_title,
        'likely_reason' : '5A'
    }
    callback_payload = _make_callback_payload(
        sitelink.qid,
        sitelink.wiki_client.dbname,
        sitelink.page.page_title,
        eval_params=eval_params,
        eval_str=eval_str
    )
    canonicalize_sitelink(sitelink.qid, sitelink.wiki_client.dbname, callback_payload)
    LOG.debug(eval_str)


def process_sitelink_title_normalization(sitelink:Sitelink) -> None:
    eval_str = f'Page "{sitelink.page.page_title}@{sitelink.wiki_client.dbname}" in {sitelink.qid} does actually exist'
    eval_params = {
        'page_exists_but_title_different' : True,
        'alt_title' : sitelink.page.page_title,
        'canonical_title' : sitelink.page.canonical_page_title,
        'likely_reason' : '5B'
    }
    callback_payload = _make_callback_payload(
        sitelink.qid,
        sitelink.wiki_client.dbname,
        sitelink.page.page_title,
        eval_params=eval_params,
        eval_str=eval_str
    )
    normalize_title(sitelink.qid, sitelink.wiki_client.dbname, sitelink.page.page_title, callback_payload)


def process_sitelink_move(sitelink:Sitelink, log_event:LogEvent, eval_str:list[str], eval_params:dict) -> None:
    moved_without_redirect = bool(int(log_event.log_params.get(b'5::noredir', b'0').decode('utf8')))
    eval_str.append(f'Moved without redirect: {moved_without_redirect}')

    move_target = log_event.log_params.get(b'4::target', b'').decode('utf8')
    move_target_namespace, _ = Page.get_namespace_from_page_title(move_target, sitelink.wiki_client.get_namespaces())
    move_source_namespace, _ = Page.get_namespace_from_page_title(sitelink.page.page_title, sitelink.wiki_client.get_namespaces())
    eval_str.append(f'Move target: {move_target} ({move_target_namespace}, from {move_source_namespace})')

    try:
        target_page_is_redirect = check_if_page_is_redirect(sitelink.wiki_client.dbname, move_target)
    except ValueError:  # this usually happens when the old logging format has been used on the client, which this script does not understand
        LOG.warn(f'Problem with {sitelink.qid}, {sitelink.wiki_client.dbname}, {sitelink.page.page_title}, {log_event}')
        return

    eval_str.append(f'Move target is redirect: {target_page_is_redirect}')

    eval_params['moved_without_redirect'] = True
    eval_params['move_target'] = move_target
    eval_params['move_target_namespace'] = move_target_namespace
    eval_params['move_source_namespace'] = move_source_namespace
    eval_params['target_page_is_redirect'] = target_page_is_redirect

    if target_page_is_redirect:
        eval_params['likely_reason'] = '1B'
    else:
        eval_params['likely_reason'] = '1A'
    eval_params['log_action'] = log_event.log_action
    eval_params['log_timestamp'] = log_event.log_timestamp

    process_sitelink_trigger_removal(sitelink, log_event, eval_str, eval_params)


def process_sitelink_delete(sitelink:Sitelink, log_event:LogEvent, eval_str:list[str], eval_params:dict) -> None:
    eval_params['missed_deletion'] = True
    eval_params['log_action'] = log_event.log_action
    eval_params['log_timestamp'] = log_event.log_timestamp

    if log_event.user.user_registration is not None and log_event.user.user_registration < log_event.log_timestamp and len(log_event.user.user_blocklog) == 0:
        eval_params['likely_reason'] = '2C, 3A, 4A, 4B'
    elif log_event.user.user_id is None:  # user does (still) not exist on Wikidata
        eval_params['likely_reason'] = '2A-a'
    elif log_event.user.user_registration is not None and log_event.user.user_registration > log_event.log_timestamp:  # user meanwhile exists on Wikidata, but didn't when it mattered
        eval_params['likely_reason'] = '2A-b'
    elif len(log_event.user.user_blocklog) > 0:
        eval_params['likely_reason'] = '2B'

    process_sitelink_trigger_removal(sitelink, log_event, eval_str, eval_params)


def process_sitelink_trigger_removal(sitelink:Sitelink, log_event:LogEvent, eval_str:list[str], eval_params:dict) -> None:
    log_event_payload = log_event.get_bot_payload_dict()
    edit_summary_log = f'; from client wiki log: page was {log_event.log_type}d by User:{log_event.actor_name}' \
                       f' on {datetime.strptime(str(log_event.log_timestamp), "%Y%m%d%H%M%S").strftime("%Y-%m-%d, %H:%M:%S")}'
    callback_payload = _make_callback_payload(sitelink.qid, sitelink.wiki_client.dbname, sitelink.page.page_title, log_event_payload, eval_params, '\n'.join(eval_str))

    remove_sitelink_from_item(
        sitelink.qid,
        sitelink.wiki_client.dbname,
        sitelink.page.page_title,
        callback_payload,
        edit_summary_log=edit_summary_log
    )

    LOG.debug('\n'.join(eval_str))
