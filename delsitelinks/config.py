import pywikibot as pwb


DB_PATH = './logging.db'

SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()
EDITSUMMARY_HASHTAG = ' #msynbotTask8'  # including leading space; may be an empty string as well
TOUCH_SLEEP = 2  # int or None; time in seconds

PAGE_IS_MISSING = 'page_is_missing'
LOCAL_QID_IS_DIFFERENT = 'local_qid_is_different'
LOCAL_QID_IS_MISSING = 'local_qid_is_missing'

QUERY_CHUNK_SIZE = 1000000
PAGES_FILENAME = './pages/{dbname}.feather'
SITELINKS_FILENAME = './sitelinks/{dbname}.feather'
PAGE_IS_MISSING_FILENAME = f'./{PAGE_IS_MISSING}/{{dbname}}.feather'
LOCAL_QID_IS_DIFFERENT_FILENAME = f'./{LOCAL_QID_IS_DIFFERENT}/{{dbname}}.feather'
LOCAL_QID_IS_MISSING_FILENAME = f'./{LOCAL_QID_IS_MISSING}/{{dbname}}.feather'

IX_URL = r'https://msbits.toolforge.org/inexistent_sitelinks_ix/{folder}/{filename}'


# These do not really response quickly enough when the logging table is queried
LARGE_WIKIS_LOGEVENTS = {
    'enwiki' : 'en.wikipedia.org',
    'srwikinews' : 'sr.wikinews.org'
}

QIDS_TO_IGNORE:list[str] = [
    'Q105429923',  # Special:RecentChanges
    'Q112333026',  # Special:ProtectedTitles
]

NEEDS_FIX_WIKIS:list[str] = [

]

RELOAD = False
