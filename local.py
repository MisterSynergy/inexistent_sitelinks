from concurrent.futures import ThreadPoolExecutor, as_completed
from glob import glob
from time import strftime
from typing import Optional

import pandas as pd
import pyarrow as pa
import requests
from memory_profiler import profile


PROJECT_SELECT_WHITELIST:Optional[list[str]] = None
PROJECT_SELECT_BLACKLIST:Optional[list[str]] = None
PROJECT_SELECT_MIN:Optional[str] = None
PROJECT_SELECT_MAX:Optional[str] = None

PRINT:bool = False
MAKE_STAT_OUTPUT:bool = False

QIDS_TO_IGNORE:list[str] = [
    'Q105429923',  # Special:RecentChanges
    'Q112333026',  # Special:ProtectedTitles
]

PAWS_BASE_URL = r'https://public.paws.wmcloud.org/User:MisterSynergy/misc/2021%2012%20deleted%20sitelinks'
FOLDER_PAGES = 'pages'
FOLDER_SITELINKS = 'sitelinks'
FOLDER_PAGE_IS_MISSING = 'page_is_missing'
FOLDER_LOCAL_QID_IS_DIFFERENT = 'local_qid_is_different'
FOLDER_LOCAL_QID_IS_MISSING = 'local_qid_is_missing'


def download_file_from_paws(url:str, folder:str, dbname:str) -> tuple:
    response = requests.get(url)

    with open(f'./{folder}/{dbname}.feather', mode='wb') as file_handle:
        file_handle.write(response.content)

    return folder, len(response.text)


def download_files_from_paws(dbname:str) -> None:
    base_url = f'{PAWS_BASE_URL}/{{folder}}/{{dbname}}.feather'
    with ThreadPoolExecutor() as executor:
        results = [ executor.submit(download_file_from_paws, base_url.format(folder=folder, dbname=dbname), folder, dbname) for folder in [ 'sitelinks', 'pages' ] ]
        for future in as_completed(results):
            folder, filesize = future.result()
            print(strftime('%Y-%m-%d, %H:%M:%S'), 'downloaded', folder, dbname, f'{filesize/1024/1024:.2f}MiB')


def load_pages(project_data:dict) -> pd.DataFrame:
    print(f'Start reading pages file for {project_data.get("dbname", "meta")} to dataframe at {strftime("%H:%M:%S")}')
    df = pd.read_feather(
        f'./{FOLDER_PAGES}/{project_data.get("dbname", "meta")}.feather'
    )
    return df


def load_sitelinks(project_data:dict) -> pd.DataFrame:
    print(f'Start reading sitelinks file for {project_data.get("dbname", "meta")} to dataframe at {strftime("%H:%M:%S")}')
    df = pd.read_feather(
        f'./{FOLDER_SITELINKS}/{project_data.get("dbname", "meta")}.feather'
    )
    return df


def make_missing_df(pages:pd.DataFrame, sitelinks:pd.DataFrame) -> pd.DataFrame:
    missing = sitelinks.merge(
        right=pages,
        how='left',
        left_on='sitelink',
        right_on='full_page_title'
    )
    missing.drop(columns=['full_page_title'], inplace=True)
    return missing


def print_df_info(df, name:str='') -> None:
    if PRINT is True:
        print(df.columns)
        print(df.info())
        print(df.describe())
        print(df.head(25))
    print(f'{name} dataframe loaded at {strftime("%H:%M:%S")}\n')


def write_stat_output(dbname:str, cnt1:int, cnt2:int, cnt3:int) -> None:
    print(f'{dbname}\t{cnt1:d}\t{cnt2:d}\t{cnt3:d}')


def make_stat_output(project_data:dict) -> None:
    pages = load_pages(project_data)
    sitelinks = load_sitelinks(project_data)

    missing = make_missing_df(pages, sitelinks)

    page_is_missing_filt = missing['ns_numerical'].isna()
    qid_is_different_filt = missing['qid'].notna() & (missing['qid']!=missing['qid_sitelink'])
    page_needs_nulledit_filt = (missing['qid'].isna() & missing['ns_numerical'].notna())

    write_stat_output(
        project_data.get('dbname', 'meta'),
        missing.loc[page_is_missing_filt].shape[0],
        missing.loc[qid_is_different_filt].shape[0],
        missing.loc[page_needs_nulledit_filt].shape[0]
    )


#@profile
def process_project(project_data:dict) -> None:
    print(f'Start script for {project_data.get("dbname", "meta")} at {strftime("%H:%M:%S")}\n')

    pages = load_pages(project_data)
    print_df_info(pages, 'pages')

    sitelinks = load_sitelinks(project_data)
    print_df_info(sitelinks, 'sitelinks')

    missing = make_missing_df(pages, sitelinks)
    print(f'Missing loaded at {strftime("%H:%M:%S")}')
    print(f'Total number of elements in missing: {missing.shape[0]}\n')

    page_is_missing_filt = missing['ns_numerical'].isna()
    missing.loc[page_is_missing_filt].reset_index().to_feather(f'./{FOLDER_PAGE_IS_MISSING}/{project_data.get("dbname", "meta")}.feather')
    print_df_info(missing.loc[page_is_missing_filt], 'inexistent pages')

    local_qid_is_different_filt = missing['qid'].notna() & (missing['qid']!=missing['qid_sitelink'])
    missing.loc[local_qid_is_different_filt].reset_index().to_feather(f'./{FOLDER_LOCAL_QID_IS_DIFFERENT}/{project_data.get("dbname", "meta")}.feather')
    print_df_info(missing.loc[local_qid_is_different_filt], 'local QID is different')

    local_qid_is_missing_filt = (missing['qid'].isna() & missing['ns_numerical'].notna())
    missing.loc[local_qid_is_missing_filt].reset_index().to_feather(f'./{FOLDER_LOCAL_QID_IS_MISSING}/{project_data.get("dbname", "meta")}.feather')
    print_df_info(missing.loc[local_qid_is_missing_filt], 'local QID is missing')

    write_stat_output(
        project_data.get('dbname', 'meta'),
        missing.loc[page_is_missing_filt].shape[0],
        missing.loc[local_qid_is_different_filt].shape[0],
        missing.loc[local_qid_is_missing_filt].shape[0]
    )

    print(f'Stop script at {strftime("%H:%M:%S")}')


def load_project_datas() -> list[dict[str, str]]:
    project_datas = []
    response = requests.get(f'{PAWS_BASE_URL}/log/wiki_clients.txt')
    for line in response.text.split('\n'):
        if line == '':
            break
        dbname, host = line.split('\t')
        project_datas.append({'dbname' : dbname, 'host' : host})
    
    return project_datas


def merge_all(directory:str=FOLDER_PAGE_IS_MISSING) -> pd.DataFrame:
    filenames = glob(f'./{directory}/*.feather')

    df_list = []
    for filename in filenames:
        df_project = pd.read_feather(filename)
        df_project['project'] = filename[len(directory)+3:-8]
        df_list.append(df_project)

    df = pd.concat(df_list, ignore_index=True)

    return df


def main() -> None:
    project_datas = load_project_datas()

    for i, project_data in enumerate(project_datas, start=1):
        if PROJECT_SELECT_WHITELIST is not None and project_data.get('dbname') not in PROJECT_SELECT_WHITELIST:
            continue
        if PROJECT_SELECT_BLACKLIST is not None and project_data.get('dbname') in PROJECT_SELECT_BLACKLIST:
            continue
        if PROJECT_SELECT_MIN is not None and project_data.get('dbname', '') < PROJECT_SELECT_MIN:
            continue
        if PROJECT_SELECT_MAX is not None and project_data.get('dbname', '') > PROJECT_SELECT_MAX:
            continue

        print(f'\n== project {i}/{len(project_datas)} ==')
        download_files_from_paws(project_data.get('dbname', 'meta'))
        process_project(project_data)
        if MAKE_STAT_OUTPUT is True:
            make_stat_output(project_data)


def main_concat() -> None:
    for directory in [ FOLDER_PAGE_IS_MISSING, FOLDER_LOCAL_QID_IS_DIFFERENT, FOLDER_LOCAL_QID_IS_MISSING ]:
        df = merge_all(directory=directory)
        
        filt = ~df['qid_sitelink'].isin(QIDS_TO_IGNORE)
        
        print(f'== Folder "{directory}" ==')
        print(f'Number of elements: {df.loc[filt].shape[0]}\nSample entries:\n')
        if df.loc[filt].shape[0] > 100:
            print(df.loc[filt].sample(100))
        else:
            print(df.loc[filt].head(df.shape[0]))

        per_project = df.loc[filt, 'project'].value_counts()
        print(per_project.head(50))

        per_item = df.loc[filt, 'qid_sitelink'].value_counts()
        print(per_item.head(50))


def count_elems(elem_name:str) -> None:
    filenames = glob(f'./{elem_name}/*.feather')

    cnt = {}
    for filename in filenames:
        df_project = pd.read_feather(filename)
        cnt[filename[len(elem_name)+3:-8]] = df_project.shape[0]

    print(f'== {elem_name} count for all projects ==')
    print(f'{sum(cnt.values())} {elem_name}\n')

    with open(f'./log_local/cnt_{elem_name}.tsv', mode='w', encoding='utf8') as file_handle:
        for key, value in cnt.items():
            file_handle.write(f'{key}\t{value}\n')


def main_count_sitelinks() -> None:
    count_elems(FOLDER_SITELINKS)


def main_count_pages() -> None:
    count_elems(FOLDER_PAGES)


def main_dbnames_comparison() -> None:
    sl_dump_columns = { 'dbname' : str, 'cnt_sitelinks_dump' : int }
    sl_online_columns = { 'dbname' : str, 'cnt_sitelinks_online' : int }

    sl_dump = pd.read_csv(
        './log_local/cnt_sitelinks.tsv',  # generated by main_count_sitelinks()
        sep='\t',
        names=sl_dump_columns.keys(),
        dtype=sl_dump_columns
    )
    sl_online = pd.read_csv(
        './log_local/cnt_sitelinks_online.tsv',  # https://quarry.wmcloud.org/query/62480
        sep='\t',
        names=sl_online_columns.keys(),
        dtype=sl_online_columns
    )

    sl_all = sl_dump.merge(right=sl_online, how='outer')

    sl_missing_in_dump = sl_all.loc[sl_all['cnt_sitelinks_dump'].isna()]
    sl_missing_online = sl_all.loc[sl_all['cnt_sitelinks_online'].isna()]

    print('== dbnames missing in dumps ==')
    print(f'* count={sl_missing_in_dump.shape[0]}')
    print(f'* total number of sitelinks={sl_missing_in_dump["cnt_sitelinks_online"].sum():.0f}')
    for tpl in sl_missing_in_dump.itertuples():
        print(tpl.dbname, f'{tpl.cnt_sitelinks_online:.0f}')

    print('\n== dbnames missing online ==')
    print(f'* count={sl_missing_online.shape[0]}')
    print(f'* total number of sitelinks={sl_missing_online["cnt_sitelinks_dump"].sum():.0f}')
    for tpl in sl_missing_online.itertuples():
        print(tpl.dbname, f'{tpl.cnt_sitelinks_dump:.0f}')


if __name__=='__main__':
    main()
#    main_concat()
#    main_count_sitelinks()
#    main_count_pages()
#    main_dbnames_comparison()