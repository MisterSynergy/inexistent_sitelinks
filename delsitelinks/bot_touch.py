import logging
from time import sleep

import pywikibot as pwb
from pywikibot.exceptions import NoUsernameError, TitleblacklistError, \
    UnknownFamilyError, UnknownSiteError, CascadeLockedPageError, \
    LockedPageError, NoPageError, APIError, OtherPageSaveError

from .config import TOUCH_SLEEP


LOG = logging.getLogger(__name__)


def touch_page(dbname:str, page_title:str) -> None:
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

    try:
        page.touch(quiet=True)
    except (APIError, NoPageError, CascadeLockedPageError, LockedPageError, OtherPageSaveError, TitleblacklistError) as exception:
        LOG.warn(exception, lang, family)
        raise RuntimeWarning from exception
    else:
        if TOUCH_SLEEP is not None:
            sleep(TOUCH_SLEEP)
