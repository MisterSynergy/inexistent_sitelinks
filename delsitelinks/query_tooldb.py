import logging
import pandas as pd

from .database import ToolDB


LOG = logging.getLogger(__name__)


def query_missing_page_df() -> pd.DataFrame:
    query = """SELECT
      CONVERT(qid_sitelink USING utf8mb4) AS qid_sitelink,
      CONVERT(sitelink USING utf8mb4) AS sitelink,
      ns_numerical,
      CONVERT(qid USING utf8mb4) AS qid
    FROM
      sitelinks
        LEFT JOIN pages ON sitelink=full_page_title
    WHERE
      ns_numerical IS NULL"""

    df = pd.DataFrame(data=ToolDB.query_tooldb(query))

    return df


def query_local_qid_is_different_df() -> pd.DataFrame:
    query = """SELECT
      CONVERT(qid_sitelink USING utf8mb4) AS qid_sitelink,
      CONVERT(sitelink USING utf8mb4) AS sitelink,
      ns_numerical,
      CONVERT(qid USING utf8mb4) AS qid
    FROM
      sitelinks
        LEFT JOIN pages ON sitelink=full_page_title
    WHERE
      ns_numerical IS NOT NULL
      AND qid!=''
      AND qid!=qid_sitelink"""

    df = pd.DataFrame(data=ToolDB.query_tooldb(query))

    return df


def query_local_qid_is_missing_df() -> pd.DataFrame:
    query = """SELECT
      CONVERT(qid_sitelink USING utf8mb4) AS qid_sitelink,
      CONVERT(sitelink USING utf8mb4) AS sitelink,
      ns_numerical,
      CONVERT(qid USING utf8mb4) AS qid
    FROM
      sitelinks
        LEFT JOIN pages ON sitelink=full_page_title
    WHERE
      ns_numerical IS NOT NULL
      AND qid=''"""

    df = pd.DataFrame(data=ToolDB.query_tooldb(query))

    return df