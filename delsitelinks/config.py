import pywikibot as pwb


DB_PATH = './logging.db'

SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()
RELOAD = False
TOUCH_SLEEP = 2 # seconds or None

QUERY_CHUNK_SIZE = 1000000
PAGES_FILENAME = './pages/{dbname}.feather'
SITELINKS_FILENAME = './sitelinks/{dbname}.feather'
PAGE_IS_MISSING_FILENAME = './page_is_missing/{dbname}.feather'
LOCAL_QID_IS_DIFFERENT_FILENAME = './local_qid_is_different/{dbname}.feather'
LOCAL_QID_IS_MISSING_FILENAME = './local_qid_is_missing/{dbname}.feather'

QIDS_TO_IGNORE:list[str] = [
    'Q105429923',  # Special:RecentChanges
    'Q112333026',  # Special:ProtectedTitles
]

NEEDS_FIX_WIKIS:list[str] = [

]