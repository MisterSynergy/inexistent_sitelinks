from dataclasses import dataclass, field
import logging
from typing import Any, Optional, Type, TypeVar
from json import JSONDecodeError

import phpserialize
import requests

from .config import LARGE_WIKIS_LOGEVENTS
from .database import Replica


LOG = logging.getLogger(__name__)
U = TypeVar('U', bound='User')


@dataclass
class Namespace:
    ns:int
    ns_local:str
    ns_generic:str
    ns_aliases:list[str] = field(default_factory=list)


@dataclass
class BlockEvent:
    log_id:int
    log_timestamp:int
    log_params:dict


@dataclass
class User:
    user_id:Optional[int] = None
    user_name:Optional[str] = None
    user_registration:Optional[int] = None
    user_editcount:Optional[int] = None
    user_blocklog:list[BlockEvent] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        if self.user_id is not None:
            self._init_blocklog()

    def _init_blocklog(self) -> None:
        if self.user_name is None:
            return

        user_name_tidied = self.user_name.replace(" ", "_").replace("'", "''")

        params = { 'username' : user_name_tidied }
        query = f"""SELECT
            log_id,
            log_timestamp,
            log_params
        FROM
            logging
        WHERE
            log_type='block'
            AND log_action='block'
            AND log_namespace=2
            AND log_title=%(username)s"""
        result = Replica.query_mediawiki('wikidatawiki', query, params=params)

        for row in result:
            self.user_blocklog.append(
                BlockEvent(
                    row['log_id'],
                    row['log_timestamp'].decode('utf8'),
                    row['log_params'].decode('utf8')
                )
            )

    def _str_blocklog(self) -> str:
        msg = f'User had {len(self.user_blocklog)} blocks'
        if len(self.user_blocklog):
            msg += f' at timestamps {self.get_block_timestamps()}'

        return msg

    def get_block_timestamps(self) -> str:
        return ', '.join([ str(block_event.log_timestamp) for block_event in self.user_blocklog ])

    def __str__(self) -> str:
        if self.user_id is None:
            return 'User does not exist at Wikidata'
        
        return f'User "{self.user_name}" has account at Wikidata since {self.user_registration}' \
               f' and made {self.user_editcount} edits; local user id {self.user_id}; block' \
               f' history: {self._str_blocklog()}'

    def get_bot_payload_dict(self) -> dict[str, Any]:
        if self.user_id is None:
            return {
                'user_id' : None,
                'user_name' : None,
                'user_registration' : None,
                'user_editcount' : None,
                'user_block_count' : None,
                'user_block_timestamps' : None
            }

        return {
            'user_id' : self.user_id,
            'user_name' : self.user_name,
            'user_registration' : self.user_registration,
            'user_editcount' : self.user_editcount,
            'user_block_count' : len(self.user_blocklog),
            'user_block_timestamps' : self.get_block_timestamps()
        }

    @classmethod
    def user_by_name(cls: Type[U], user_name:str) -> U:
        user_name_tidied = user_name.replace("'", "''")
        
        params = { 'username' : user_name_tidied }
        query_exists = f"""SELECT
            user_id,
            user_name,
            user_registration,
            user_editcount
        FROM
            user
        WHERE
            user_name=%(username)s"""
        result_exists = Replica.query_mediawiki('wikidatawiki', query_exists, params=params)

        if len(result_exists)==0:
            return cls()

        return cls(
            user_id=result_exists[0]['user_id'],
            user_name=result_exists[0]['user_name'].decode('utf8'),
            user_registration=int(result_exists[0]['user_registration'].decode('utf8')),
            user_editcount=result_exists[0]['user_editcount'],
        )


@dataclass
class LogEvent:
    log_id:int
    log_timestamp:int
    log_type:str
    log_action:str
    actor_name:str
    log_params:dict
    user:User = field(init=False)

    def __post_init__(self) -> None:
        self._init_loguser()

    def _init_loguser(self) -> None:
        self.user = User.user_by_name(self.actor_name)

    def get_bot_payload_dict(self) -> dict[str, Any]:
        return {
            'timestamp' : self.log_timestamp,
            'actor_name' : self.actor_name,
            'params' : self.log_params,
            'type' : self.log_type,
            'action' : self.log_action
        }


@dataclass
class WikiClient:
    dbname:str
    hostname:str
    namespaces:list[Namespace] = field(default_factory=list)
    lazy_namespaces:Optional[bool]=False

    def __post_init__(self) -> None:
        if self.lazy_namespaces is not True:
            self._init_namespaces()
    
    def _init_namespaces(self) -> None:
        request_params = {
            'action' : 'query',
            'meta' : 'siteinfo',
            'siprop' : 'namespaces|namespacealiases',
            'formatversion' : '2',
            'format' : 'json'
        }
        payload = WikiClient.api_request(self.hostname, request_params)

        for data in payload.get('query', {}).get('namespaces', {}).values():
            ns_id = data.get('id')
            ns_local = data.get('name')
            ns_generic = data.get('canonical')
            ns_aliasses = []

            for alias_data in payload.get('query', {}).get('namespacealiases', []):
                if alias_data.get('id') != ns_id:
                    continue
                ns_aliasses.append(alias_data.get('alias'))

            self.namespaces.append(
                Namespace(
                    ns_id,
                    ns_local,
                    ns_generic,
                    ns_aliasses
                )
            )

    def get_namespace_by_id(self, id:int, ns_generic:bool=False) -> str:
        if len(self.namespaces) == 0:
            self._init_namespaces()

        for namespace in self.namespaces:
            if namespace.ns != id:
                continue

            if ns_generic is True:
                return namespace.ns_generic

            return namespace.ns_local

        return ''  # assume main namespace otherwise; TODO: is this okay?

    def get_namespaces(self) -> list[Namespace]:
        if len(self.namespaces) == 0:
            self._init_namespaces()
        
        return self.namespaces

    @staticmethod
    def api_request(host:str, request_params:dict) -> dict:
        response = requests.get(
            url=f'https://{host}/w/api.php',
            params=request_params
        )

        if response.status_code not in [ 200 ]:
            raise RuntimeError(f'Cannot retrieve namespaces from MWAPI; HTTP status {response.status_code}')

        try:
            payload = response.json()
        except JSONDecodeError as exception:
            raise RuntimeError('Cannot parse JSON response') from exception

        return payload


@dataclass
class Page:
    page_title:str
    wiki_client:WikiClient
    page_namespace:Optional[Namespace] = None
    qid_local:Optional[str] = None  # the value from page_props table
    log_events:list[LogEvent] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self._init_logevents()
    
    def _init_logevents(self) -> None:
        log_actions = [
            { 'type' : 'delete', 'action' : 'delete' },
            { 'type' : 'move', 'action' : 'move' },
            { 'type' : 'move', 'action' : 'move_redir' },
        ]

        for log_dict in log_actions:
            self.log_events = [ *self.log_events, *self._get_log_events(log_dict) ]

    def _get_log_events(self, log:dict[str, str]) -> list[LogEvent]:  # TODO: tidy
        page_namespace, plain_page_title = Page.get_namespace_from_page_title(
            self.page_title,
            self.wiki_client.get_namespaces()
        )
        
        # TODO: figure out whether this can be done one way or the other for all projects
        if self.wiki_client.dbname not in LARGE_WIKIS_LOGEVENTS.keys():
            params = {
                'logtype' : log.get('type', ''),
                'logaction' : log.get('action', ''),
                'logtitle' : plain_page_title.replace(' ', '_').replace('"', '""'),
                'lognamespace' : page_namespace
            }
            query = f"""SELECT
                log_id,
                log_timestamp,
                actor_name,
                log_params
            FROM
                logging_userindex
                    JOIN actor_logging ON log_actor=actor_id
            WHERE
                log_type=%(logtype)s
                AND log_action=%(logaction)s
                AND log_title=%(logtitle)s
                AND log_namespace=%(lognamespace)s"""

            result = Replica.query_mediawiki(self.wiki_client.dbname, query, params=params)
        else:
            api_response = WikiClient.api_request(
                LARGE_WIKIS_LOGEVENTS.get(self.wiki_client.dbname, 'meta.wikimedia.org'),
                {
                    'action': 'query',
                    'format': 'json',
                    'list': 'logevents',
                    'leprop': 'ids',
                    'leaction': f'{log.get("type", "")}/{log.get("action", "")}',
                    'letitle': self.page_title
                }
            )

            logids:list[int] = []
            for dct in api_response.get('query', {}).get('logevents', []):
                logids.append(dct.get('logid'))       

            if len(logids) == 0:
                result = []
            else:
                params_tuple = tuple(logids)
                query = f"""SELECT
                    log_id,
                    log_timestamp,
                    actor_name,
                    log_params
                FROM
                    logging_userindex
                        JOIN actor_logging ON log_actor=actor_id
                WHERE
                    log_id IN ({', '.join(['?' for _ in logids])})"""  # [str(logid) for logid in logids]

                result = Replica.query_mediawiki(
                    self.wiki_client.dbname,
                    query,
                    params_tuple=params_tuple
                )

        log_events:list[LogEvent] = []

        if len(result) == 0 or len(result[0]) == 0:
            return log_events

        for row in result:
            log_id = row['log_id']
            log_timestamp = row['log_timestamp']
            actor_name = row['actor_name'].decode('utf8')
            
            try:
                log_params = phpserialize.loads(row['log_params'])
            except ValueError as exception:  # old log_params format, to be ignored
                if row['log_params'] is None:
                    log_params = {}
                else:
                    log_params = { 'oldformat' : row['log_params'].decode('utf8') }
            
            # comes back either way from some wikis; convert here if necessary
            if isinstance(log_timestamp, bytes) or isinstance(log_timestamp, bytearray):
                log_timestamp = log_timestamp.decode('utf8')

            log_timestamp_processed = int(log_timestamp)
            log_events.append(
                LogEvent(
                    log_id,
                    log_timestamp_processed,
                    log.get('type', ''),
                    log.get('action', ''),
                    actor_name,
                    log_params
                )
            )

        return log_events

    @property
    def lastest_log_event(self) -> Optional[LogEvent]:
        latest_log_event:Optional[LogEvent] = None

        for log_event in self.log_events:
            if latest_log_event is None or latest_log_event.log_timestamp < log_event.log_timestamp:
                latest_log_event = log_event
        
        return latest_log_event


    @property
    def alternative_page_titles(self) -> list[str]:
        alt_titles:list[str] = []

        ns, title = self.__class__.get_namespace_from_page_title(self.page_title, self.wiki_client.get_namespaces())
        if ns == 0:
            return alt_titles

        for namespace in self.wiki_client.get_namespaces():
            if namespace.ns != ns:
                continue
            alt_titles.append(f'{namespace.ns_generic}:{title}')
            for ns_alias in namespace.ns_aliases:
                alt_titles.append(f'{ns_alias}:{title}')

        return alt_titles
    
    @property
    def canonical_page_title(self) -> str:
        ns, title = self.__class__.get_namespace_from_page_title(self.page_title, self.wiki_client.get_namespaces())

        if ns == 0:
            return self.page_title

        for namespace in self.wiki_client.get_namespaces():
            if namespace.ns == ns:
                return f'{namespace.ns_local}:{title}'

        return self.page_title


    @staticmethod
    def get_namespace_from_page_title(page_title:str, namespaces:list[Namespace]) -> tuple[int, str]:
        if ':' not in page_title:
            return 0, page_title

        page_title_parts = page_title.split(':')

        for namespace in namespaces:
            if page_title_parts[0]==namespace.ns_local or page_title_parts[0]==namespace.ns_generic or page_title_parts[0] in namespace.ns_aliases:
                return namespace.ns, ':'.join(page_title_parts[1:])

        return 0, page_title  # page title contains a colon, but not a namespace identifier --- thus assume main namespace


@dataclass
class Sitelink:
    qid:str  # from wikibase wb_items_per_site
    wiki_client:WikiClient
    page:Page
