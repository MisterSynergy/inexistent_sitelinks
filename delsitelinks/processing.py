from time import strftime
from datetime import datetime
from typing import Any, Optional
import pandas as pd

from .config import QIDS_TO_IGNORE, PAGE_IS_MISSING, LOCAL_QID_IS_DIFFERENT, LOCAL_QID_IS_MISSING, COMPUTE_OFFLINE
from .query import load_file_from_ix
from .datahandler import load_pages, load_sitelinks
from .helper import get_missing_filter, write_to_stat_file
from .types import WikiClient, User, Page, Sitelink, LogEvent
from .bot import touch_page, remove_sitelink_from_item, canonicalize_sitelink, check_if_item_has_sitelink, check_if_page_exists_on_client, check_if_page_is_redirect, normalize_title


def process_project(wiki_client:WikiClient, job_remove_sitelinks:bool=False, job_qid_different:bool=False, job_qid_missing:bool=False, job_stat_file:bool=True) -> None:
    if COMPUTE_OFFLINE is True:  # this requires offline computations for all projects
        if job_stat_file is True or job_remove_sitelinks is True:
            page_is_missing = load_file_from_ix(PAGE_IS_MISSING, f'{wiki_client.dbname}.feather')
        if job_stat_file is True or job_qid_different is True:
            local_qid_is_different = load_file_from_ix(LOCAL_QID_IS_DIFFERENT, f'{wiki_client.dbname}.feather')
        if job_stat_file is True or job_qid_missing is True:
            local_qid_is_missing = load_file_from_ix(LOCAL_QID_IS_MISSING, f'{wiki_client.dbname}.feather')
    else:
        pages = load_pages(wiki_client)
        sitelinks = load_sitelinks(wiki_client)

        missing = sitelinks.merge(
            right=pages,
            how='left',
            left_on='sitelink',
            right_on='full_page_title'
        )
        missing.drop(columns=['full_page_title'], inplace=True)

        if job_stat_file is True or job_remove_sitelinks is True:
            filt_page_is_missing = get_missing_filter(missing, PAGE_IS_MISSING)
            page_is_missing = missing.loc[filt_page_is_missing]
        if job_stat_file is True or job_qid_different is True:
            filt_local_qid_is_different = get_missing_filter(missing, LOCAL_QID_IS_DIFFERENT)
            local_qid_is_different = missing.loc[filt_local_qid_is_different]
        if job_stat_file is True or job_qid_missing is True:
            filt_local_qid_is_missing = get_missing_filter(missing, LOCAL_QID_IS_MISSING)
            local_qid_is_missing = missing.loc[filt_local_qid_is_missing]

    if job_remove_sitelinks is True:
        remove_sitelinks(page_is_missing, wiki_client)
        
    if job_qid_different is True:
        touch_different_local_qids(local_qid_is_different, wiki_client)
    
    if job_qid_missing is True:
        touch_missing_local_qids(local_qid_is_missing, wiki_client)
        
    if job_stat_file is True:
        write_to_stat_file(
            wiki_client.dbname,
            {
                PAGE_IS_MISSING : page_is_missing.shape[0],
                LOCAL_QID_IS_DIFFERENT : local_qid_is_different.shape[0],
                LOCAL_QID_IS_MISSING : local_qid_is_missing.shape[0]
            }
        )


def process_sitelink_no_logevent(sitelink:Sitelink) -> None:  # TODO: does nothing
    eval_str = f'Cannot find a log timestamp for page "{sitelink.page.page_title}" on {sitelink.wiki_client.dbname} in {sitelink.qid}'
    print(eval_str)


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
    print(eval_str)


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
        print(f'Problem with {sitelink.qid}, {sitelink.wiki_client.dbname}, {sitelink.page.page_title}, {log_event}')
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

    print('\n'.join(eval_str))


def process_sitelink(sitelink:Sitelink) -> None:  # TODO: tidy
    if sitelink.qid in QIDS_TO_IGNORE:
        return

    try:
        item_has_sitelink = check_if_item_has_sitelink(sitelink.qid, sitelink.wiki_client.dbname, sitelink.page.page_title)
    except RuntimeWarning:
        return
    if not item_has_sitelink:
        if sitelink.page.page_title in sitelink.page.alternative_page_titles:
            process_sitelink_alt_title(sitelink)
            return
        else:
            print(f'Item {sitelink.qid} does not have a sitelink "{sitelink.page.page_title}" for {sitelink.wiki_client.dbname}')
            return # nothing to do

    try:
        page_exists = check_if_page_exists_on_client(sitelink.wiki_client.dbname, sitelink.page.page_title)
    except ValueError:
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
    print(f'Cases of sitelinks to inexistent pages in {wiki_client.dbname}: {df.shape[0]}')
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


def touch_different_local_qids(df:pd.DataFrame, wiki_client:WikiClient) -> None:
    print(f'Page needs nulledit in {wiki_client.dbname} because the local qid in the page_props table is different: {df.shape[0]}')
    for i, elem in enumerate(df.itertuples(), start=1):
        try:
            touch_page(
                wiki_client.dbname,
                elem.sitelink
            )
        except RuntimeWarning:
            print(f'did not touch "{elem.sitelink}" on project {wiki_client.dbname}')
        else:
            if i%100==0:
                print(f'touched "{elem.sitelink}" on project {wiki_client.dbname} ({i}/{df.shape[0]}, {strftime("%Y-%m-%d, %H:%M:%S")})')


def touch_missing_local_qids(df:pd.DataFrame, wiki_client:WikiClient) -> None:
    print(f'Pages need nulledit in {wiki_client.dbname} because page_props value is missing: {df.shape[0]}')
    for i, elem in enumerate(df.itertuples(), start=1):
        try:
            touch_page(
                wiki_client.dbname,
                elem.sitelink
            )
        except RuntimeWarning:
            print(f'did not touch "{elem.sitelink}" on project {wiki_client.dbname}')
        else:
            if i%100==0:
                print(f'touched "{elem.sitelink}" on project {wiki_client.dbname} ({i}/{df.shape[0]}, {strftime("%Y-%m-%d, %H:%M:%S")})')
