import logging
from typing import Any, Optional

import pywikibot as pwb
from pywikibot.exceptions import NoUsernameError, InvalidTitleError, UnsupportedPageError, \
    UnknownFamilyError, UnknownSiteError, APIError, OtherPageSaveError, SiteDefinitionError, \
    NoPageError, InconsistentTitleError

from .config import REPO, EDITSUMMARY_HASHTAG
from .database import LoggingDB


LOG = logging.getLogger(__name__)


def check_if_item_has_sitelink(qid:str, dbname:str, page_title:str) -> bool:
    q_item = pwb.ItemPage(REPO, qid)
    if not q_item.exists():
        return False

    if q_item.isRedirectPage():
        return False

    try:
        q_item.get()
    except NoPageError:
        return False

    if not q_item.sitelinks:
        return False

    try:
        connected_sitelink = q_item.sitelinks.get(dbname)
    except NoUsernameError as exception:
        raise RuntimeWarning from exception
    else:
        if connected_sitelink is not None and page_title==connected_sitelink.canonical_title():
            return True

    return False


def check_if_page_exists_on_client(dbname:str, page_title:str) -> bool:
    family, lang = str(pwb.site.APISite.fromDBName(dbname)).split(':')

    project_page = pwb.Page(
        pwb.Site(code=lang, fam=family),
        page_title
    )

    try:
        return project_page.exists()
    except (InvalidTitleError, UnsupportedPageError, SiteDefinitionError, InconsistentTitleError) as exception:
        LOG.warn(f'{dbname}:{page_title} is skipped due to {exception}')

    return False


def check_if_page_is_redirect(dbname:str, page_title:str) -> bool:
    family, lang = str(pwb.site.APISite.fromDBName(dbname)).split(':')
    project_page = pwb.Page(
        pwb.Site(code=lang, fam=family),
        page_title
    )

    try:
        return project_page.isRedirectPage()
    except (InvalidTitleError, UnsupportedPageError) as exception:
        LOG.warn(f'{dbname}:{page_title} is skipped due to {exception}')

    return False


def _make_edit_log(page:pwb.page.BasePage, err:Optional[Exception]=None) -> None:
    if err is not None:  # something went wrong --- nothing to log
        return

    LoggingDB.insert_log(page.latest_revision.get('revid'), page.callback_payload)


def _make_edit_summary(dbname:str, page_title:str, edit_summary_log:Optional[str]=None) -> str:
    if edit_summary_log is None:
        edit_summary_log = ''

    return f'remove sitelink "{dbname}:{page_title}" (page does not exist on client wiki{edit_summary_log}) #{dbname}{EDITSUMMARY_HASHTAG}'


def remove_sitelink_from_item(qid:str, dbname:str, page_title:str, callback_payload:dict[str, Any], edit_summary_log:Optional[str]=None) -> None:
    edit_summary = _make_edit_summary(dbname, page_title, edit_summary_log)

    q_item = pwb.ItemPage(REPO, qid)
    q_item.callback_payload = callback_payload  # payload for logging purposes
    q_item.removeSitelink(
        dbname,
        summary=edit_summary,
        callback=_make_edit_log
    )


def handle_uncanonicalizable_sitelink(qid:str, dbname:str, callback_payload:dict[str, Any], sitelink:pwb.page.BaseLink) -> bool:
    client_page = pwb.Page(sitelink)

    if not client_page.exists():
        callback_payload['eval_str'] += f'\nuncanonicalizable sitelink: found {sitelink.canonical_title()} for {dbname} in {qid} (inexistent)'
        callback_payload['eval_params']['likely_reason'] = '5A-1'
        remove_sitelink_from_item(qid, dbname, sitelink.canonical_title(), callback_payload)
        return True

    if client_page.isRedirectPage():
        callback_payload['eval_str'] += f'\nuncanonicalizable sitelink: found {sitelink.canonical_title()} for {dbname} in {qid} (redirect)'
        callback_payload['eval_params']['likely_reason'] = '5A-2'
        remove_sitelink_from_item(qid, dbname, sitelink.canonical_title(), callback_payload)
        return True

    possible_other_q_item = pwb.ItemPage.fromPage(client_page, lazy_load=True)
    if possible_other_q_item.exists():
        callback_payload['eval_str'] += f'\nuncanonicalizable sitelink: found {sitelink.canonical_title()} for {dbname} in {qid} (connected to {possible_other_q_item.title()})'
        callback_payload['eval_params']['likely_reason'] = '5A-3'
        remove_sitelink_from_item(qid, dbname, sitelink.canonical_title(), callback_payload)
        return True

    return False


def canonicalize_sitelink(qid:str, dbname:str, callback_payload:dict[str, Any]) -> None:
    edit_summary = f'normalize sitelink for {dbname} by using the canonical namespace prefix #{dbname}{EDITSUMMARY_HASHTAG}'

    q_item = pwb.ItemPage(REPO, qid)
    try:
        q_item.get()
    except NoPageError:
        return

    connected_sitelink = q_item.sitelinks.get(dbname)
    if connected_sitelink is None:
        return

    already_done = handle_uncanonicalizable_sitelink(qid, dbname, callback_payload, connected_sitelink)
    if already_done is True:
        return

    q_item.callback_payload = callback_payload  # payload for logging purposes

    try:
        q_item.setSitelink(
            {
                'site' : dbname,
                'title' : connected_sitelink.canonical_title()
            },
            summary=edit_summary,
            callback=_make_edit_log
        )
    except (APIError, OtherPageSaveError) as exception:
        pass


def normalize_title(qid:str, dbname:str, page_title:str, callback_payload:dict[str, Any]) -> None:
    family, lang = str(pwb.site.APISite.fromDBName(dbname)).split(':')
    try:
        site = pwb.Site(lang, family)
    except (UnknownFamilyError, UnknownSiteError) as exception:
        LOG.warn(exception, lang, family)
        raise RuntimeWarning from exception

    try:
        page = pwb.Page(site, page_title)
    except NoUsernameError as exception:
        LOG.warn(exception, lang, family)
        raise RuntimeWarning from exception

    q_item = pwb.ItemPage(REPO, qid)
    q_item.callback_payload = callback_payload  # payload for logging purposes

    if page_title == page.title():
        return

    try:
        q_item.setSitelink(
            {
                'site' : dbname,
                'title' : page.title()
            },
            summary='Normalize sitelink title to match spelling on client wiki',
            callback=_make_edit_log
        )
    except (APIError, OtherPageSaveError) as exception:
        pass
