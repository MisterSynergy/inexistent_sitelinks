import pywikibot as pwb
from typing import Optional

# bot editing related
SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()
EDITSUMMARY_HASHTAG:str = ' #msynbotTask8'  # including leading space; may be an empty string as well
TOUCH_SLEEP:int = 2  # int or None; time in seconds

# logging
DB_PATH:str = './logging.db'  # an sqlite3 database to log actions performed on the wiki
TOOLDB_NAME_FILE:str = './tooldb.my.cnf'  # sitting in the main directory of the tool
SPECIAL_PAGE_LOG:str = './special_page_log.tsv'  # sitting in the main directory of the tool

# querying
QUERY_CHUNK_SIZE:int = 500000  # chunksize when querying from replicas; done in order to reduce memory demands

# These do not really response quickly enough when the logging table is queried
LARGE_WIKIS_LOGEVENTS:dict[str, str] = {  # TODO: list instead of dict, and retrieve url from meta replica
    'enwiki' : 'en.wikipedia.org',
    'srwikinews' : 'sr.wikinews.org',
}

# max number of sitelinks removed per project
MAX_SITELINKS_PER_PROJECT = 1000

# Q-IDs to ignore at most places; these should likely only be items for Special pages which are rather unusual
QIDS_TO_IGNORE:list[str] = [
]

# wiki dbnames of projects with persistent problems; might require server admin attention as in
# https://phabricator.wikimedia.org/T311148
NEEDS_FIX_WIKIS:list[str] = []

# project selection: which projects to work on if not all (both None); if a project is listed in both lists, the blacklist wins
# only work on these projects (dbname); list or None
WORK_WHITELIST:Optional[list[str]] = None

# do not work on these projects (dbname); list or None
WORK_BLACKLIST:Optional[list[str]] = None

# 
MIN_PROJECT:Optional[str] = None

# 
MAX_PROJECT:Optional[str] = None

# when using touch tasks, select which ones to activate here
TOUCH_QID_DIFFERENT:bool = True
TOUCH_QID_MISSING:bool = False
