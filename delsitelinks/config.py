import pywikibot as pwb
from typing import Optional

DB_PATH:str = './logging.db'

SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()
EDITSUMMARY_HASHTAG:str = ' #msynbotTask8'  # including leading space; may be an empty string as well
TOUCH_SLEEP:int = 2  # int or None; time in seconds

PAGE_IS_MISSING:str = 'page_is_missing'
LOCAL_QID_IS_DIFFERENT:str = 'local_qid_is_different'
LOCAL_QID_IS_MISSING:str = 'local_qid_is_missing'

QUERY_CHUNK_SIZE:int = 1000000
PAGES_FILENAME:str = './pages/{dbname}.feather'
SITELINKS_FILENAME:str = './sitelinks/{dbname}.feather'
PAGE_IS_MISSING_FILENAME:str = f'./{PAGE_IS_MISSING}/{{dbname}}.feather'
LOCAL_QID_IS_DIFFERENT_FILENAME:str = f'./{LOCAL_QID_IS_DIFFERENT}/{{dbname}}.feather'
LOCAL_QID_IS_MISSING_FILENAME:str = f'./{LOCAL_QID_IS_MISSING}/{{dbname}}.feather'

LOG_STATFILE:str = './log/stat.tsv'
LOG_WIKICLIENTS:str = './log/wiki_clients.txt'

IX_URL:str = r'https://msbits.toolforge.org/inexistent_sitelinks_ix/{folder}/{filename}'


# These do not really response quickly enough when the logging table is queried
LARGE_WIKIS_LOGEVENTS:dict[str, str] = {
    'enwiki' : 'en.wikipedia.org',
    'srwikinews' : 'sr.wikinews.org'
}

# Q-IDs to ignore at most places; these should likely only be items for Special pages which are rather unusual
QIDS_TO_IGNORE:list[str] = [
    'Q105429923',  # Special:RecentChanges
    'Q112333026',  # Special:ProtectedTitles
]

# wiki dbnames of projects with persistent problems; might require server admin attention as in
# https://phabricator.wikimedia.org/T311148
NEEDS_FIX_WIKIS:list[str] = [

]

# if True, then update existing dumps from database; otherwise just load from dumped file
RELOAD:bool = True

# when using touch tasks, select which ones to activate here
TOUCH_QID_DIFFERENT:bool = True
TOUCH_QID_MISSING:bool = False

# project selection: which projects to work on if not all (both None); if a project is listed in both lists, the blacklist wins
# only work on these projects (dbname); list or None
WORK_WHITELIST:Optional[list[str]] = None

# do not work on these projects (dbname); list or None
WORK_BLACKLIST:Optional[list[str]] = None

MIN_PROJECT:Optional[str] = None
MAX_PROJECT:Optional[str] = None