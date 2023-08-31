import os
import sys
import re
import json
import lochness
import logging
import requests
import lochness.net as net
import collections as col
import lochness.tree as tree
from pathlib import Path
import pandas as pd
import datetime
from typing import List, Union
import tempfile as tf
from lochness.redcap.process_piis import process_and_copy_db

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


logger = logging.getLogger(__name__)


def get_field_names_from_redcap(api_url: str,
                                api_key: str,
                                study_name: str) -> list:
    '''Return all field names from redcap database'''

    record_query = {
        'token': api_key,
        'content': 'exportFieldNames',
        'format': 'json',
    }

    # pull field names from REDCap for the study
    content = post_to_redcap(api_url,
                             record_query,
                             f'initializing data {study_name}')

    # load pulled information as list of dictionary : data
    with tf.NamedTemporaryFile(suffix='tmp.json') as tmpfilename:
        lochness.atomic_write(tmpfilename.name, content)
        with open(tmpfilename.name, 'r') as f:
            data = json.load(f)

    field_names = []
    for item in data:
        field_names.append(item['original_field_name'])

    return field_names


def remove_file_that_may_exist(file_path: Path) -> None:
    '''Remove a file that may exist'''
    try:
        os.remove(file_path)
    except:
        pass


def initialize_metadata(Lochness: 'Lochness object',
                        study_name: str,
                        redcap_id_colname: str,
                        redcap_consent_colname: str,
                        multistudy: bool = True,
                        upenn: bool = False) -> None:
    '''Initialize metadata.csv by pulling data from REDCap for Pronet network

    Key arguments:
        Lochness: Lochness object
        study_name: Name of the study, str. eg) PronetLA
        redcap_id_colname: Name of the ID field name in REDCap, str.
        redcap_consent_colname: Name of the consent date field name in REDCap,
                                str.
        multistudy: True if the redcap repo contains multisite data, bool.
        upenn: True if upenn redcap is included in the source list, bool.
    '''
    # specific to DPACC project
    site_code_study = study_name[-2:]  # 'LA'
    project_name = study_name.split(site_code_study)[0]  # 'Pronet'

    # metadata study location
    general_path = Path(Lochness['phoenix_root']) / 'GENERAL'
    metadata_study = general_path / study_name / f"{study_name}_metadata.csv"

    # use redcap_project function to load the redcap keyrings for the project
    _, api_url, api_key = next(redcap_projects(
        Lochness, study_name, f'redcap.{project_name}'))

    # sources to add to the metadata, apart from REDCap, XNAT, and Box
    source_source_name_dict = {'mindlamp': ['Mindlamp', 'chrdbb_lamp_id']}

    # to extract ID and consent form for all the records that
    # belong to the site from screening & baseline arms
    record_query = {
        'token': api_key,
        'content': 'record',
        'format': 'json',
        'fields[0]': redcap_id_colname,
        'fields[1]': redcap_consent_colname,
        'events[0]': 'screening_arm_1',
        'events[1]': 'screening_arm_2',
        'events[2]': 'baseline_arm_1',
        'events[3]': 'baseline_arm_2',
        'filterLogic': f"contains([{redcap_id_colname}],'{site_code_study}')"
        }


    # to add mindlamp source ID to the record query
    for num, (source, (source_name, source_field_name)) in \
            enumerate(source_source_name_dict.items()):
        record_query[f"fields[{2+num}]"] = source_field_name

    # pull all records from the project's REDCap repo
    content = post_to_redcap(api_url,
                             record_query,
                             f'initializing data {study_name}')

    # load pulled information as a list of dictionaries
    with tf.NamedTemporaryFile(suffix='tmp.json') as tmpfilename:
        lochness.atomic_write(tmpfilename.name, content)
        with open(tmpfilename.name, 'r') as f:
            data = json.load(f)

    # replace empty string as None
    df = pd.DataFrame(data).replace('', None)

    # if empty REDCap
    if len(df) == 0:
        logger.warn(f'There are no records for {site_code_study}')
        remove_file_that_may_exist(metadata_study)
        return

    # only keep AMPSCZ rows
    df = df[df[redcap_id_colname].str.match('[A-Z]{2}\d{5}')]

    # if no data matches AMPSCZ ID
    if len(df) == 0:
        logger.warn(f'There are no records for {site_code_study}')
        remove_file_that_may_exist(metadata_study)
        return

    # make a single row for each subject record
    # redcap_event_name column contains timepoint information (different arms)
    df.drop('redcap_event_name', axis=1, inplace=True)

    df_final = pd.DataFrame()
    for subject, df_tmp in df.groupby(redcap_id_colname):
        df_new = pd.concat(
            [df_tmp[col].dropna().reset_index(drop=True) for col in df_tmp],
            axis=1)
        df_final = pd.concat([df_final, df_new], axis=0)

    # drop if consent date is missing
    df_final = df_final[
            ~df_final[redcap_consent_colname].isnull()].reset_index()

    # skip no data has consent date
    if len(df_final) == 0:
        logger.warn(f'There are no records for {site_code_study}')
        remove_file_that_may_exist(metadata_study)
        return

    df = pd.DataFrame()

    # extract subject ID and source IDs for each sources
    for index, row in df_final.iterrows():
        subject_id = row[redcap_id_colname]
        # Subject ID
        subject_dict = {'Subject ID': subject_id, 'Study': site_code_study}

        # Consent date
        subject_dict['Consent'] = row[redcap_consent_colname]

        # Redcap default information
        subject_dict['REDCap'] = \
                f'redcap.{project_name}:{subject_id}'
        subject_dict['REDCap'] += \
                f';redcap.UPENN:{row[redcap_id_colname]}'  # UPENN REDCAP
        subject_dict['Box'] = f'box.{study_name}:{subject_id}'
        subject_dict['XNAT'] = f'xnat.{study_name}:*:{subject_id}'

        # for the datatype, which requires ID extraction from REDCap
        for source, (source_name, source_field_name) \
                in source_source_name_dict.items():
            if pd.isnull(row[source_field_name]):
                pass
            else:
                value = row[source_field_name]
                subject_dict[source_name] = f"{source}.{study_name}:{value}"

        subject_df_tmp = pd.DataFrame.from_dict(subject_dict, orient='index')
        df = pd.concat([df, subject_df_tmp.T])

    # register all of the lables as active
    df['Active'] = 1

    # reorder columns to match lochness metadata format
    main_cols = ['Active', 'Consent', 'Subject ID']
    df = df[main_cols + \
            [x for x in df.columns if x not in main_cols]]

    # only overwrite when there is an update in the data
    if metadata_study.is_file():
        target_df = pd.read_csv(metadata_study)
    else:
        target_df = pd.DataFrame()

    same_df = df.reset_index(drop=True).equals(target_df)

    if same_df:
        pass
    else:
        df.to_csv(metadata_study, index=False)


def initialize_metadata_rm(Lochness: 'Lochness object',
                           study_name: str,
                           redcap_id_colname: str,
                           redcap_consent_colname: str,
                           multistudy: bool = True,
                           upenn: bool = False) -> None:
    '''Initialize metadata.csv by pulling data from REDCap for Pronet network

    Key arguments:
        Lochness: Lochness object
        study_name: Name of the study, str. eg) PronetLA
        redcap_id_colname: Name of the ID field name in REDCap, str.
        redcap_consent_colname: Name of the consent date field name in REDCap,
                                str.
        multistudy: True if the redcap repo contains multisite data, bool.
        upenn: True if upenn redcap is included in the source list, bool.
    '''
    # specific to DPACC project
    site_code_study = study_name[-2:]  # 'LA'
    project_name = study_name.split(site_code_study)[0]  # 'Pronet'

    # metadata study location
    general_path = Path(Lochness['phoenix_root']) / 'GENERAL'
    metadata_study = general_path / study_name / f"{study_name}_metadata.csv"

    # use redcap_project function to load the redcap keyrings for the project
    _, api_url, api_key = next(redcap_projects(
        Lochness, study_name, f'redcap.{project_name}'))

    # sources to add to the metadata, apart from REDCap, XNAT, and Box
    source_source_name_dict = {'mindlamp': ['Mindlamp', 'chrdbb_lamp_id']}

    # to extract ID and consent form for all the records that
    # belong to the site from screening & baseline arms
    record_query = {
        'token': api_key,
        'content': 'record',
        'format': 'json',
        'fields[0]': redcap_id_colname,
        'fields[1]': redcap_consent_colname,
        'events[0]': 'screening_arm_1',
        'events[1]': 'screening_arm_2',
        'events[2]': 'baseline_arm_1',
        'events[3]': 'baseline_arm_2',
        'filterLogic': f"contains([{redcap_id_colname}],'{site_code_study}')"
        }


    # to add mindlamp source ID to the record query
    for num, (source, (source_name, source_field_name)) in \
            enumerate(source_source_name_dict.items()):
        record_query[f"fields[{2+num}]"] = source_field_name

    # pull all records from the project's REDCap repo
    content = post_to_redcap(api_url,
                             record_query,
                             f'initializing data {study_name}')

    # load pulled information as a list of dictionaries
    with tf.NamedTemporaryFile(suffix='tmp.json') as tmpfilename:
        lochness.atomic_write(tmpfilename.name, content)
        with open(tmpfilename.name, 'r') as f:
            data = json.load(f)

    # replace empty string as None
    df = pd.DataFrame(data).replace('', None)

    # if empty REDCap
    if len(df) == 0:
        logger.warn(f'There are no records for {site_code_study}')
        remove_file_that_may_exist(metadata_study)
        return

    # only keep AMPSCZ rows
    df = df[df[redcap_id_colname].str.match('[A-Z]{2}\d{5}')]

    # if no data matches AMPSCZ ID
    if len(df) == 0:
        logger.warn(f'There are no records for {site_code_study}')
        remove_file_that_may_exist(metadata_study)
        return

    # make a single row for each subject record
    # redcap_event_name column contains timepoint information (different arms)
    df.drop('redcap_event_name', axis=1, inplace=True)

    df_final = pd.DataFrame()
    for subject, df_tmp in df.groupby(redcap_id_colname):
        df_new = pd.concat(
            [df_tmp[col].dropna().reset_index(drop=True) for col in df_tmp],
            axis=1)
        df_final = pd.concat([df_final, df_new], axis=0)

    # drop if consent date is missing
    df_final = df_final[
            ~df_final[redcap_consent_colname].isnull()].reset_index()

    # skip no data has consent date
    if len(df_final) == 0:
        logger.warn(f'There are no records for {site_code_study}')
        remove_file_that_may_exist(metadata_study)
        return

    df = pd.DataFrame()

    # extract subject ID and source IDs for each sources
    for subject_id, table in df_final.groupby(redcap_id_colname):
        # Subject ID
        subject_dict = {'Subject ID': subject_id, 'Study': site_code_study}

        # Consent date
        subject_dict['Consent'] = table.iloc[0][redcap_consent_colname]

        # Redcap default information
        subject_dict['REDCap'] = \
                f'redcap.{project_name}:{subject_id}'
        subject_dict['REDCap'] += \
                f';redcap.UPENN:{subject_id}'  # UPENN REDCAP
        subject_dict['Box'] = f'box.{study_name}:{subject_id}'
        subject_dict['XNAT'] = f'xnat.{study_name}:*:{subject_id}'

        # for the datatype, which requires ID extraction from REDCap
        for source, (source_name, source_field_name) \
                in source_source_name_dict.items():
            if pd.isnull(table[source_field_name]).all():
                pass
            else:
                id_table = table[~pd.isnull(table[source_field_name])]
                most_recent_index = id_table['redcap_repeat_instance']
                value = id_table.loc[most_recent_index, source_field_name]
                subject_dict[source_name] = f"{source}.{study_name}:{value}"

        subject_df_tmp = pd.DataFrame.from_dict(subject_dict, orient='index')
        df = pd.concat([df, subject_df_tmp.T])

    # register all of the lables as active
    df['Active'] = 1

    # reorder columns to match lochness metadata format
    main_cols = ['Active', 'Consent', 'Subject ID']
    df = df[main_cols + \
            [x for x in df.columns if x not in main_cols]]

    # only overwrite when there is an update in the data
    if metadata_study.is_file():
        target_df = pd.read_csv(metadata_study)
    else:
        target_df = pd.DataFrame()

    same_df = df.reset_index(drop=True).equals(target_df)

    if same_df:
        pass
    else:
        df.to_csv(metadata_study, index=False)


def get_run_sheets_for_datatypes_rm(api_url, api_key,
                                    redcap_subject, id_field,
                                    json_path: Union[Path, str]) -> None:
    if not json_path.is_file():
        return

    with open(json_path, 'r') as fp:
        json_data = json.load(fp)

    raw_path = Path(json_path).parent.parent

    mod_run_sheet_name_dict = {
            'eeg':['eeg_run_sheet'],
            'mri':['mri_run_sheet'],
            'interviews':['speech_sampling_run_sheet'],
            'phone':['digital_biomarkers_mindlamp_onboarding',
                     'digital_biomarkers_mindlamp_checkin'],
            'actigraphy':['digital_biomarkers_axivity_onboarding',
                          'digital_biomarkers_axivity_checkin'],
            'surveys': ['penncnb'] }

    for modality, run_sheet_names in mod_run_sheet_name_dict.items():
        for run_sheet_name in run_sheet_names:
            run_sheet_dicts = [x for x in json_data
                    if x['redcap_repeat_instrument'] == run_sheet_name]

            # if no run sheet
            if len(run_sheet_dicts) == 0:
                continue

            # content_df = pd.DataFrame(run_sheet_dicts)
            # content_df = pd.DataFrame()
            # loop through repeated run sheet dicts
            for content_dict in run_sheet_dicts:
                repeat_instance = content_dict['redcap_repeat_instance']
                content_df = pd.DataFrame.from_dict(content_dict,
                                                    orient='index',
                                                    columns=['field_value'])
                content_df.index.name = 'field_name'
                # content_df = pd.concat([content_df, content_df_tmp])

            # if all is empty, or 0
            all_empty = ((content_df[content_df.columns[0]]=='0') |
                         (content_df[content_df.columns[0]]=='')).all()

            if all_empty:
                continue  # don't have if all empty

            raw_modality_path = raw_path / modality
            raw_modality_path.mkdir(exist_ok=True, parents=True)
            output_name = Path(json_path).name.split('.json')[0]

            if modality == 'surveys':  # run sheet for PENN CNB
                run_sheet_output = raw_modality_path / \
                   f'{output_name}.Run_sheet_PennCNB_{repeat_instance}.csv'

            else:
                if run_sheet_name.endswith('checkin'):
                    run_sheet_output = raw_modality_path / \
                       f'{output_name}.Run_sheet_{modality}' \
                       f'_checkin_{repeat_instance}.csv'
                else:
                    run_sheet_output = raw_modality_path / \
                       f'{output_name}.' \
                       f'Run_sheet_{modality}_{repeat_instance}.csv'

            if run_sheet_output.is_file():
                target_df = pd.read_csv(run_sheet_output, index_col=0)
                with tf.NamedTemporaryFile(suffix='tmp.csv') as tmp_file:
                    content_df.to_csv(tmp_file.name)
                    check_df = pd.read_csv(tmp_file.name, index_col=0)
                
                same_df = check_df.equals(target_df)

                if same_df:
                    print('Not saving run sheet')
                    continue

            content_df.to_csv(run_sheet_output)
            os.chmod(run_sheet_output, 0o0755)


def get_run_sheets_for_datatypes(api_url, api_key,
                                 redcap_subject, id_field,
                                 json_path: Union[Path, str]) -> None:
    '''Extract run sheet information from REDCap JSON and save as csv file

    For each data types, there should a record of the data acquisition in the
    REDCap json file. This information is extracted and saved as a csv flie in
        PHOENIX/PROTECTED/raw/
            {STUDY}/{DATATYPE}/{subject}.{study}.Run_sheet_{DATATYPE}.csv

    Some measures will have 'repeated instrument' configuration on the REDCap,
    which will lead the REDCap exported json file to have a different format.
    There will be extra list of dictionaries for each repeated instrument. The
    'redcap_repeated_instrument' item in the list labels which measure the list
    is for. For example, when there are two repeated run sheets for MRI, there
    would be two more lists of dictionaries included in the REDCap-json file. 
    Each list will be labelled as "mri_run_sheet" in
    "redcap_repeated_instrument", while "redcap_event_name" will be
    "baseline_arm_1". Each list will have information about of the repeated run
    sheets.

    Key Arguments:
        - json_path: REDCap json path, Path.

    Returns:
        - None
    '''
    if not json_path.is_file():
        return

    raw_path = Path(json_path).parent.parent

    mod_run_sheet_name_dict = {
            'eeg':['eeg_run_sheet'],
            'mri':['mri_run_sheet'],
            'interviews':['speech_sampling_run_sheet'],
            'phone':['digital_biomarkers_mindlamp_onboarding',
                     'digital_biomarkers_mindlamp_checkin'],
            'actigraphy':['digital_biomarkers_axivity_onboarding',
                          'digital_biomarkers_axivity_checkin'],
            'surveys': ['penncnb'] }

    for modality, run_sheet_names in mod_run_sheet_name_dict.items():
        for run_sheet_name in run_sheet_names:
            redcap_subject_sl = redcap_subject.lower()

            metadata_query = {'token': api_key,
                    'content': 'record',
                    'format': 'json',
                    'forms[0]': run_sheet_name,
                    'filterLogic': f"[{id_field}] = '{redcap_subject}' or "
                                   f"[{id_field}] = '{redcap_subject_sl}'",
                    'rawOrLabel': 'raw',
                    }

            try:
                content = post_to_redcap(api_url, metadata_query, '').decode(
                        'utf-8')
            except:
                print(f"{redcap_subject}: {run_sheet_name} does not exist"
                      " in REDCap")
                continue

            content_dict_list = json.loads(content)

            # select the right form, and ignore the weird empty dictionary
            content_dict_list = [x for x in content_dict_list if
                    x[run_sheet_name+'_complete'] != '']

            # for run sheet at each timepoint - baseline, follow up1, etc.
            # content_num is set to start with 1 to match the session number
            for content_num, content_dict in enumerate(content_dict_list, 1):
                content_df = pd.DataFrame.from_dict(content_dict,
                                                    orient='index',
                                                    columns=['field_value'])
                content_df.index.name = 'field_name'

                # if all is empty, or 0
                all_empty = ((content_df[content_df.columns[0]]=='0') |
                             (content_df[content_df.columns[0]]=='')).all()

                if all_empty:
                    continue  # don't have if all empty

                raw_modality_path = raw_path / modality
                raw_modality_path.mkdir(exist_ok=True, parents=True)
                output_name = Path(json_path).name.split('.json')[0]
                
                # output run sheet path
                suffix = '' if content_num == 1 else f"_{content_num}"
                if modality == 'surveys':  # run sheet for PENN CNB
                    run_sheet_output = raw_modality_path / \
                       f'{output_name}.Run_sheet_PennCNB_{content_num}.csv'

                else:
                    if run_sheet_name.endswith('checkin'):
                        run_sheet_output = raw_modality_path / \
                           f'{output_name}.Run_sheet_{modality}' \
                           f'_checkin_{content_num}.csv'
                    else:
                        run_sheet_output = raw_modality_path / \
                           f'{output_name}.' \
                           f'Run_sheet_{modality}_{content_num}.csv'

                if run_sheet_output.is_file():
                    target_df = pd.read_csv(run_sheet_output, index_col=0)
                    with tf.NamedTemporaryFile(suffix='tmp.csv') as tmp_file:
                        content_df.to_csv(tmp_file.name)
                        check_df = pd.read_csv(tmp_file.name, index_col=0)
                    
                    same_df = check_df.equals(target_df)

                    if same_df:
                        print('Not saving run sheet')
                        continue

                content_df.to_csv(run_sheet_output)
                os.chmod(run_sheet_output, 0o0755)


def check_if_modified(subject_id: str,
                      existing_json: str,
                      df: pd.DataFrame) -> bool:
    '''Check if subject data has been modified in the data entry trigger db

    Comparing unix times of the json modification and lastest redcap update
    '''

    json_modified_time = Path(existing_json).stat().st_mtime  # in unix time

    subject_df = df[df.record == subject_id]

    # if the subject does not exist in the DET_DB, return False
    if len(subject_df) < 1:
        return False

    lastest_update_time = subject_df.loc[
            subject_df['timestamp'].idxmax()].timestamp

    if lastest_update_time > json_modified_time:
        return True
    else:
        return False


def get_data_entry_trigger_df(Lochness: 'Lochness',
                              study: str) -> pd.DataFrame:
    '''Read Data Entry Trigger database as dataframe

    Key Arguments:
        Lochness: Lochness config object, obj.
        study: study string, str. eg) PronetYA

    Returns:
        pandas dataframe for data entry trigger database
    '''
    if 'redcap' in Lochness:
        if 'data_entry_trigger_csv' in Lochness['redcap'][study]:
            db_loc = Lochness['redcap'][study]['data_entry_trigger_csv']
            if Path(db_loc).is_file():
                db_df = pd.read_csv(db_loc)
                try:
                    db_df['record'] = db_df['record'].astype(str)
                except KeyError:
                    db_df = pd.DataFrame({'record':[]})
                return db_df

    db_df = pd.DataFrame({'record':[]})
    return db_df


@net.retry(max_attempts=5)
def sync(Lochness, subject, dry=False):

    logger.debug(f'exploring {subject.study}/{subject.id}')
    deidentify = deidentify_flag(Lochness, subject.study)

    logger.debug(f'deidentify for study {subject.study} is {deidentify}')

    for redcap_instance, redcap_subject in iterate(subject):
        for redcap_project, api_url, api_key in redcap_projects(
                Lochness, subject.study, redcap_instance):
            # process the response content
            _redcap_project = re.sub(r'[\W]+', '_', redcap_project.strip())

            # default location to protected folder
            dst_folder = tree.get('surveys',
                                  subject.protected_folder,
                                  processed=False,
                                  BIDS=Lochness['BIDS'],
                                  makedirs=True)
            fname = f'{redcap_subject}.{_redcap_project}.json'
            dst = Path(dst_folder) / fname

            # PII processed content to general processed
            proc_folder = tree.get('surveys',
                                   subject.general_folder,
                                   processed=True,
                                   BIDS=Lochness['BIDS'],
                                   makedirs=True)

            proc_dst = Path(proc_folder) / fname

            # Data Entry Trigger
            # check if the data has been updated by checking the redcap data
            # entry trigger db
            # UPENN redcap does not have data download limit, therefore no DET
            if 'UPENN' in redcap_instance:
                pass
            else:
                if dst.is_file():
                    # load dataframe for redcap data entry trigger
                    db_df = get_data_entry_trigger_df(Lochness, subject.study)

                    if check_if_modified(redcap_subject, dst, db_df):
                        pass  # if modified, download REDCap data
                    else:
                        logger.debug(f"{subject.study}/{subject.id} "
                                     "No DET updates")
                        break  # if not modified, don't pull

            logger.debug(f"Downloading REDCap ({redcap_instance}) data")
            _debug_tup = (redcap_instance, redcap_project, redcap_subject)

            if 'UPENN' in redcap_instance:
                # UPENN REDCap is set up with its own record_id, but have added
                # "src_subject_id" field to note AMP-SCZ ID
                redcap_subject_sl = redcap_subject.lower()
                digits = [1, 2, 3, 4, 5, 6, 7, 8, 9]
                digits_str = [str(x) for x in digits]
                contains_logic = []
                for subject_id in [redcap_subject, redcap_subject_sl]:
                    contains_logic += [
                            f"contains([src_subject_id], '{subject_id}_{x}')"
                            for x in digits_str]
                    contains_logic += [
                            f"contains([src_subject_id], '{subject_id}={x}')"
                            for x in digits_str]


                record_query = {
                    'token': api_key,
                    'content': 'record',
                    'format': 'json',
                    'filterLogic': f"[src_subject_id] = '{redcap_subject}' or "
                                   f"[src_subject_id] = '{redcap_subject_sl}' or "
                                   f"{' or '.join(contains_logic)}"
                }

            else:
                id_field = Lochness['redcap_id_colname']
                redcap_subject_sl = redcap_subject.lower()
                record_query = {
                    'token': api_key,
                    'content': 'record',
                    'format': 'json',
                    'records[0]': redcap_subject,
                    'records[1]': redcap_subject_sl
                   }

            # post query to redcap
            content = post_to_redcap(api_url,
                                     record_query,
                                     _debug_tup)

            # check if response body is nothing but a sad empty array
            if content.strip() == b'[]':
                logger.info(f'no redcap data for {redcap_subject}')
                continue

            content_dict_list = json.loads(content)
            if deidentify:
                # get fields that contains PII
                metadata_query = {'token': api_key,
                                  'content': 'metadata',
                                  'format': 'json'}
                content = post_to_redcap(api_url,
                                         metadata_query,
                                         _debug_tup)

                metadata = json.loads(content)
                for content_dict in content_dict_list:
                    for field in metadata:
                        if field['identifier'] == 'y':
                            try:
                                content_dict.pop(field['field_name'])
                            except:
                                pass

            content = json.dumps(content_dict_list).encode('utf-8')

            if not dry:
                if not os.path.exists(dst):
                    logger.debug(f'saving {dst}')
                    lochness.atomic_write(dst, content)
                    # Extract run sheet information
                    if 'UPENN' in redcap_instance:
                        continue
                    get_run_sheets_for_datatypes(api_url, api_key,
                                                 subject.id, id_field,
                                                 dst)
                    # process_and_copy_db(Lochness, subject, dst, proc_dst)
                    # update_study_metadata(subject, json.loads(content))

                else:
                    # responses are not stored atomically in redcap
                    crc_src = lochness.crc32(content.decode('utf-8'))
                    crc_dst = lochness.crc32file(dst)

                    if crc_dst != crc_src:
                        logger.info('different - crc32: downloading data')
                        logger.warn(f'file has changed {dst}')
                        # lochness.backup(dst)
                        logger.debug(f'saving {dst}')
                        lochness.atomic_write(dst, content)

                        if 'UPENN' in redcap_instance:
                            continue
                        # Extract run sheet information
                        get_run_sheets_for_datatypes(api_url, api_key,
                                                     subject.id, id_field,
                                                     dst)
                        # process_and_copy_db(Lochness, subject, dst, proc_dst)
                        # update_study_metadata(subject, json.loads(content))
                    else:
                        logger.info('No new update in newly downloaded '
                                    f'content for {redcap_subject}. '
                                    'Not saving the data')
                        # update the dst file's mtime so it can prevent the
                        # same file being pulled from REDCap
                        # os.utime(dst)


class REDCapError(Exception):
    pass


def save_redcap_metadata(Lochness, subject):
    # get fields that contains PII
    for redcap_instance, redcap_subject in iterate(subject):
        for redcap_project, api_url, api_key in redcap_projects(
                Lochness, subject.study, redcap_instance):

            if 'UPENN' in redcap_instance:
                continue
            _debug_tup = (redcap_instance, redcap_project, redcap_subject)
            metadata_query = {'token': api_key,
                              'content': 'metadata',
                              'format': 'csv',
                              'returnFormat': 'json'}
            content = post_to_redcap(api_url,
                                     metadata_query,
                                     _debug_tup)

            meta_data_dst = Path(Lochness['phoenix_root']) / 'GENERAL' / \
                    'redcap_metadata.csv'
            crc_src = lochness.crc32(content.decode('utf-8'))
            if meta_data_dst.is_file():
                crc_dst = lochness.crc32file(meta_data_dst)
                if crc_dst != crc_src:
                    logger.info('metadata different - crc32: downloading data')
                    lochness.atomic_write(meta_data_dst, content)
            else:
                lochness.atomic_write(meta_data_dst, content)


def redcap_projects(Lochness, phoenix_study, redcap_instance):
    '''get redcap api_url and api_key for a phoenix study

    Key Arguments:
        Lochness: Lochness object.
        phoenix_study: name of the study, str. eg) PronetLA
        redcap_instance: name of the redcap field for the study in the keyring,
                         str. eg) redcap.PronetLA

    Yields:
        project: name of the redcap project field in the keyring file, str.
                 eg) "Pronet" when the lochness keyring file has
                     "redcap.Pronet" : {"URL": ***, API_TOKEN: ...}
        api_url: REDCap API url, str
        api_key: REDCap API key, str
    '''
    Keyring = Lochness['keyring']

    # Check for mandatory keyring items
    # part 1 - checking for REDCAP field right below the 'lochness' in keyring
    if 'REDCAP' not in Keyring['lochness']:
        raise KeyringError("lochness > REDCAP not found in keyring")

    # part 2 - check for study under 'REDCAP' field
    if phoenix_study not in Keyring['lochness']['REDCAP']:
        raise KeyringError(f'lochness > REDCAP > {phoenix_study}'
                           'not found in keyring')

    if redcap_instance not in Keyring['lochness']['REDCAP'][phoenix_study]:
        raise KeyringError(f'lochness > REDCAP > {phoenix_study} '
                           f'> {redcap_instance} not found in keyring')

    # part 3 - checking for redcap_instance
    if redcap_instance not in Keyring:
        raise KeyringError(f"{redcap_instance} not found in keyring")

    if 'URL' not in Keyring[redcap_instance]:
        raise KeyringError(f"{redcap_instance} > URL not found in keyring")

    if 'API_TOKEN' not in Keyring[redcap_instance]:
        raise KeyringError(f"{redcap_instance} > API_TOKEN "
                           " not found in keyring")

    # get URL
    api_url = Keyring[redcap_instance]['URL'].rstrip('/') + '/api/'

    # begin generating project,api_url,api_key tuples
    for project in Keyring['lochness']['REDCAP']\
            [phoenix_study][redcap_instance]:
        if project not in Keyring[redcap_instance]['API_TOKEN']:
            raise KeyringError(f"{redcap_instance} > API_TOKEN > {project}"
                               " not found in keyring")
        api_key = Keyring[redcap_instance]['API_TOKEN'][project]
        yield project, api_url, api_key


def post_to_redcap(api_url, data, debug_tup):
    r = requests.post(api_url, data=data, stream=True, verify=False)
    if r.status_code != requests.codes.OK:
        raise REDCapError(f'redcap url {r.url} responded {r.status_code}')
    content = r.content

    # you need the number bytes read before any decoding
    content_len = r.raw._fp_bytes_read

    # verify response content integrity
    if 'content-length' not in r.headers:
        logger.warn('server did not return a content-length header, '
                    f'can\'t verify response integrity for {debug_tup}')
    else:
        expected_len = int(r.headers['content-length'])
        if content_len != expected_len:
            raise REDCapError(
                    f'content length {content_len} does not match '
                    f'expected length {expected_len} for {debug_tup}')
    return content


class KeyringError(Exception):
    pass


def deidentify_flag(Lochness, study):
    ''' get study specific deidentify flag with a safe default '''
    value = Lochness.get('redcap', dict()) \
                    .get(study, dict()) \
                    .get('deidentify', False)

    # if this is anything but a boolean, just return False
    if not isinstance(value, bool):
        return False
    return value


def iterate(subject):
    '''generator for redcap instance and subject'''
    for instance, ids in iter(subject.redcap.items()):
        for id_inst in ids:
            yield instance, id_inst



def update_study_metadata(subject, content: List[dict]) -> None:
    '''update metadata csv based on the redcap content: source_id'''

    sources = ['XNAT', 'Box', 'Mindlamp', 'Mediaflux', 'Daris']

    orig_metadata_df = pd.read_csv(subject.metadata_csv)

    subject_bool = orig_metadata_df['Subject ID'] == subject.id
    subject_index = orig_metadata_df[subject_bool].index
    subject_series = orig_metadata_df.loc[subject_index]
    other_metadata_df = orig_metadata_df[~subject_bool]

    updated = False
    for source in sources:
        if f"{source.lower()}_id" in content[0]:  # exist in the redcap
            source_id = content[0][f"{source.lower()}_id"]
            if source not in subject_series:
                subject_series[source] = f'{source.lower()}.{source_id}'
                updated = True

            # subject already has the information
            elif subject_series.iloc[0][source] != \
                    f'{source.lower()}.{source_id}':
                subject_series.iloc[0][source] = \
                        f'{source.lower()}.{source_id}'
                updated = True
            else:
                pass

    if updated:
        new_metadata_df = pd.concat([other_metadata_df, subject_series])

        # overwrite metadata
        new_metadata_df.to_csv(subject.metadata_csv, index=False)


if __name__ == '__main__':
    # testing purposes
    # config_loc = '/mnt/prescient/Prescient_data_sync/config.yml'
    from lochness.config import load
    config_loc = '/mnt/ProNET/Lochness/config.yml'
    Lochness = load(config_loc)

    # get URL
    keyring = Lochness['keyring']
    api_url = keyring['redcap.Pronet']['URL'] + '/api/'
    api_key = keyring['redcap.Pronet']['API_TOKEN']['Pronet']
    print(api_url, api_key)
    subjects = list(lochness.read_phoenix_metadata(Lochness, ['PronetYA']))
    subject = subjects[1]

    sync(Lochness, subject)

    # break
    # id_field = Lochness['redcap_id_colname']
    # for subject_path in (
            # Path(Lochness['phoenix_root']) / 'PROTECTED').glob('*/raw/*'):
        # subject = subject_path.name
        # print(subject)
        # json_path = subject_path / 'surveys' / f'{subject}.Pronet.json'
        # get_run_sheets_for_datatypes(
                # api_url, api_key, subject, id_field, json_path)

