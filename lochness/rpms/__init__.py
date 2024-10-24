"""
Module to import data from RPMS
"""
import collections as col
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Union

import pandas as pd
import yaml

import lochness.net as net
import lochness.tree as tree
from lochness.db.crawlers import metadata as metadata_crawler

pd.set_option('mode.chained_assignment', None)


yaml.SafeDumper.add_representer(
    col.OrderedDict, yaml.representer.SafeRepresenter.represent_dict)
logger = logging.getLogger(__name__)


def _wait():
    
    # RPMS dumps data at 20:00 (AEDT) or at 19:00 (AEST)
    tz=os.getenv('TIME_ZONE','AEDT')
    RPMS_DUMP_HOUR=20 if tz=='AEDT' else 19

    # calculate _sleep in minutes
    # it is between 15 (7:55 pm) and 0 (8:10 pm)
    _sleep=0
    now=datetime.now()
    if now.hour==RPMS_DUMP_HOUR-1:
        if now.minute>=55:
            _sleep=15-(now.minute-55)

    elif now.hour==RPMS_DUMP_HOUR:
        if now.minute<=10:
            _sleep=10-now.minute

    # argument of sleep() is in seconds
    sleep(_sleep*60)


def get_rpms_database(rpms_root_path: str) -> Dict[str, pd.DataFrame]:
    '''Return dictionary of RPMS database in pandas dataframes

    Based on the date in the file name, the most recent csv file exported by
    the RPMS is loaded as a dictionary to be stored in the phoenix directory.
    Other old csv files are moved to a temporary directory.

    Key arguments:
        rpms_root_path: root of the RPMS sync directory, str.

    Returns:
        all_df_dict: all measures loaded as pandas dataframe in dict.
                     key: name of the measure extracted from the file name.
                     value: pandas dataframe of the measure database
    '''
    all_df_dict = {}
    measure_date_dict = {}

    rpms_old_files_root = Path(rpms_root_path) / 'old_files'
    rpms_old_files_root.mkdir(exist_ok=True)
    
    measure_file_df = pd.DataFrame()
    for measure_file in Path(rpms_root_path).glob('*csv'):
        # measure_name = measure_file.name.split('.')[0]
        rpms_pattern = re.compile(
                r'PrescientStudy_Prescient_(\w+)_(\d{2}.\d{2}.\d{4}).csv',
                re.IGNORECASE)

        pattern_search = re.search(rpms_pattern, Path(measure_file).name)
        if not pattern_search:
            continue
        measure_name = pattern_search.group(1)
        measure_file_date = pd.to_datetime(pattern_search.group(2),
                                           dayfirst=True)
        measure_file_df_tmp = pd.DataFrame({
            'measure_file': [measure_file],
            'measure_name': measure_name,
            'measure_file_date': measure_file_date})
        measure_file_df = pd.concat([measure_file_df, measure_file_df_tmp])
    
    for measure_name, table in measure_file_df.groupby('measure_name'):
        n = 0
        for _, row in table.sort_values(
                'measure_file_date', ascending=False).iterrows():
            if n == 0:
                try:
                    # do not read CSV files within 7:55-8:10 pm
                    # they are exported by RPMS and modified by Tashrif's programs
                    # during this window
                    _wait()
                    df_tmp = pd.read_csv(row.measure_file, dtype=str)
                except pd.errors.EmptyDataError:  # ignore csv is empty
                    shutil.move(row.measure_file,
                                rpms_old_files_root / row.measure_file.name)

                all_df_dict[measure_name] = df_tmp
                measure_date_dict[measure_name] = row.measure_file_date
            else:
                shutil.move(row.measure_file,
                            rpms_old_files_root / row.measure_file.name)
            n += 1

    return all_df_dict


def get_run_sheets_for_datatypes(target_df_loc: Union[Path, str]) -> None:
    '''Extract run sheet information from RPMS outputs and save as csv file

    For each data types, there should be Run Sheets completed by RAs on RPMS
    This information is extracted and saved as a csv flie in the 
        PHOENIX/PROTECTED/raw/
            {STUDY}/{DATATYPE}/{subject}.{study}.Run_sheet_{DATATYPE}.csv

    Key Arguments:
        - target_df_loc: subject RPMS csv path, Path.

    Returns:
        - None
    '''
    # target_df_loc = Path(target_df_loc)
    # if not target_df_loc.is_file():
        # return

    modality_fieldname_dict = {'eeg': 'eeg_run_sheet',
                               'actigraphy': 'Actigraphy',
                               'mri': 'mri_run_sheet',
                               'surveys': 'penncnb',
                               'interviews': 'speech_sampling_run_sheet'}

    for modality, fieldname in modality_fieldname_dict.items():
        if target_df_loc.name.endswith(f"_{fieldname}.csv"):
            print(modality, fieldname)
            raw_subject_path = target_df_loc.parent.parent
            subject = raw_subject_path.name
            study = 'Prescient' + subject[:2]
            raw_modality_path = raw_subject_path / modality
            raw_modality_path.mkdir(exist_ok=True, parents=True)

            if modality == 'surveys':
                run_sheet_output_prefix = raw_modality_path / \
                        f'{subject}.{study[:-2]}.Run_sheet_PennCNB'
            else:
                run_sheet_output_prefix = raw_modality_path / \
                        f'{subject}.{study[:-2]}.Run_sheet_{modality}'

            # convert month to datapoint
            if modality in ['eeg', 'mri']:
                # two month : first timepoint
                # four month : second timpeoint
                time_to_timepoint = {2: 1, 4: 2}
            elif fieldname == 'PennCNB':
                time_to_timepoint = {2: 1, 4: 2, 8: 3, 14: 4, 16: 5}
            else:  # actigraphy & EMA
                time_to_timepoint = dict(zip(range(2, 15), range(1, 14)))

            # save run sheet
            target_df = pd.read_csv(target_df_loc)
            target_df['timepoint'] = target_df['visit'].map(time_to_timepoint)
            for tp, table in target_df.groupby('timepoint'):
                run_sheet_output = f'{run_sheet_output_prefix}_{int(tp)}.csv'

                # compare existing table
                if Path(run_sheet_output).is_file():
                    run_sheet_prev = pd.read_csv(
                        run_sheet_output, dtype=str).reset_index(drop=True)
                    same_df = table.reset_index(drop=True).astype(str).equals(
                            run_sheet_prev.astype(str))
                    if same_df:
                        continue

                table.to_csv(run_sheet_output, index=False)
                #os.chmod(run_sheet_output, 0o0755)


def initialize_metadata(Lochness: 'Lochness object',
                        study_name: str,
                        rpms_id_colname: str,
                        rpms_consent_colname: str,
                        multistudy: bool = True,
                        upenn: bool = False) -> None:
    '''Initialize metadata.csv by pulling data from RPMS for Prescient project

    Key arguments:
        Lochness: Lochness object
        study_name: Name of the study, str.
        rpms_id_colname: Name of the ID field name in RPMS, str.
        rpms_consent_colname: Name of the consent date field name in RPMS,
                                str.
        multistudy: True if the rpms repo contains more than one study's data
        upenn: True if upenn redcap is included in the source list, bool.

    Note:
        Currently, this function only adds REDCap information for the UPENN
        Cognitive battery REDCap, and does not support adding other redcap
        sources into the metadata.
    '''
    rpms_root_path = Lochness['RPMS_PATH']

    # get list of csv files from the rpms root
    all_df_dict = get_rpms_database(rpms_root_path)

    df = pd.DataFrame()

    ids_with_consent = all_df_dict['informed_consent_run_sheet'][
            ~all_df_dict['informed_consent_run_sheet'][
                rpms_consent_colname].isnull()][
                        Lochness['RPMS_id_colname']].tolist()

    # test ids - if it's a production lochness ignore test IDs
    if Lochness.get('ignore_id_csv', False):
        ignore_id_list = pd.read_csv(Lochness.get('ignore_id_csv'))[
                'id'].tolist()
    else:
        ignore_id_list = []

    # inclusion list - if it's a development lochness only get IDs
    if Lochness.get('id_list_csv', False):
        id_list = pd.read_csv(Lochness.get('id_list_csv'))[
                'id'].tolist()
    else:
        id_list = []

    # case_with_consent
    subject_with_consent = []
    for measure, df_measure_all_subj in all_df_dict.items():
        if rpms_consent_colname in df_measure_all_subj.columns:
            subject_with_consent = df_measure_all_subj[
                ~df_measure_all_subj[rpms_consent_colname].isnull()][
                        rpms_id_colname].unique()


    # for measure, df_measure_all_subj in all_df_dict.items():
    for df_measure_all_subj in [
            all_df_dict['informed_consent_run_sheet'],
            all_df_dict['digital_biomarkers_mindlamp_onboarding']]:
        df_measure_all_subj = df_measure_all_subj[
            df_measure_all_subj[rpms_id_colname].isin(subject_with_consent)
            ]

        # get the site information from the study name, eg. PrescientAD
        site_code_study = study_name[-2:]  # 'AD'
        project_name = study_name.split(site_code_study)[0]  # 'Prescient'

        # loop through each subject in the RPMS database
        if not rpms_id_colname in df_measure_all_subj.columns:
            continue

        consent_dict = {}
        for subject, df_table in df_measure_all_subj.groupby(
                rpms_id_colname):
            for index, df_measure in df_table.iterrows():
                if not df_measure[rpms_id_colname] in ids_with_consent:
                    continue

                # ignore if testcase
                if df_measure[rpms_id_colname] in ignore_id_list:
                    continue

                # if ids to pull exist
                if len(id_list) > 0:
                    # don't pull if it's not in the list
                    if df_measure[rpms_id_colname] not in id_list:
                        continue

                # if the rpms table is not ready
                # (e.g.doesn't have the subject col)
                if rpms_id_colname not in df_measure.index or \
                    pd.isna(df_measure[rpms_id_colname]):
                    continue

                site_code_rpms_id = df_measure[rpms_id_colname][:2]

                # if the subject does not belong to the site, pass it
                if site_code_rpms_id != site_code_study:
                    continue

                subject_dict = {'Subject ID': df_measure[rpms_id_colname],
                                'Study': site_code_study}

                # if mindlamp_id exists in the rpms table
                if 'chrdbb_lamp_id' in df_measure:
                    # most_recent_lamp_id = df_table
                    df_table['LastModifiedDate'] = pd.to_datetime(
                            df_table['LastModifiedDate'])
                    df_table = df_table.sort_values('LastModifiedDate')
                    most_recent_lamp_id = df_table.iloc[-1][
                            'chrdbb_lamp_id']
                    if not pd.isna(most_recent_lamp_id):
                        subject_dict['Mindlamp'] = \
                            f'mindlamp.{study_name}:{most_recent_lamp_id}'

                # Consent date
                if rpms_consent_colname in df_measure.index:
                    consent_tmp = datetime.strptime(
                            df_measure[rpms_consent_colname],
                            '%d/%m/%Y %I:%M:%S %p')

                    if subject in consent_dict.keys():
                        prev_consent = datetime.strptime(
                            consent_dict[subject], '%Y-%m-%d')
                        if prev_consent > consent_tmp:
                            subject_dict['Consent'] = consent_tmp.strftime(
                                    '%Y-%m-%d')
                            consent_dict[subject] = subject_dict['Consent']
                        else:
                            subject_dict['Consent'] = prev_consent.strftime(
                                '%Y-%m-%d')
                    else:
                        subject_dict['Consent'] = consent_tmp.strftime(
                                '%Y-%m-%d')
                        consent_dict[subject] = subject_dict['Consent']

                # mediaflux source has its foldername as its subject ID
                subject_dict['RPMS'] = f'rpms.{study_name}:' + \
                                       df_measure[rpms_id_colname]
                subject_dict['Mediaflux'] = f'mediaflux.{study_name}:' + \
                                            df_measure[rpms_id_colname]

                if upenn:
                    subject_dict['REDCap'] = \
                        'redcap.UPENN:' + df_measure[rpms_id_colname]
                    subject_dict['REDCap'] += \
                        ';redcap.UPENN_nda:' + df_measure[rpms_id_colname]

                df_tmp = pd.DataFrame.from_dict(subject_dict, orient='index')
                df = pd.concat([df, df_tmp.T])

    # if there is no data for the study, return without saving metadata
    if len(df) == 0:
        return

    # Each subject may have more than one arms, which will result in more than
    # single item for the subject in the RPMS pulled `content`
    # remove empty lables
    df_final = pd.DataFrame()

    for _, table in df.groupby(['Subject ID']):
        table['Consent'] = pd.to_datetime(table['Consent']).min()
        pad_filled = table.fillna(
                method='ffill').fillna(method='bfill').iloc[0]
        df_final = pd.concat([df_final, pad_filled], axis=1)
    df_final = df_final.T

    # register all of the lables as active
    df_final['Active'] = 1

    # reorder columns
    main_cols = ['Active', 'Consent', 'Subject ID']
    df_final = df_final[main_cols + \
            [x for x in df_final.columns if x not in main_cols]]

    # import into DB
    metadata_crawler.import_metadata_df(
        lochness_config=Lochness,
        metadata_df=df_final,
        study_id=study_name
    )

    general_path = Path(Lochness['phoenix_root']) / 'PROTECTED'
    metadata_study = general_path / study_name / f"{study_name}_metadata.csv"

    df_final.to_csv(metadata_study, index=False)


def get_subject_data(all_df_dict: Dict[str, pd.DataFrame],
                     subject: object,
                     id_colname: str) -> Dict[str, pd.DataFrame]:
    '''Get subject data from the pandas dataframes in the dictionary'''

    subject_df_dict = {}
    for measure, measure_df in all_df_dict.items():
        if id_colname not in measure_df.columns:
            continue

        measure_df[id_colname] = measure_df[id_colname].astype(str)
        subject_df = measure_df[measure_df[id_colname] == subject.id]

        # RPMS should overwrite the row whenever there is an update in any 
        # field of the visit. The snippet below is a safety measure, to store
        # most recent visit row for each visit
        # if 'visit' in subject_df.columns:
            # for unique_visit, table in subject_df.groupby('visit'):
                # if len(table) == 1 or 'Row#' in subject_df:
                    # pass
                # # entry_status form does not have LastModifiedDate
                # elif measure == 'entry_status':
                    # pass
                # else:
                    # pass
                    # most_recent_row_index = pd.to_datetime(
                            # table['LastModifiedDate']).idxmax()
                    # non_recent_row_index = [x for x in table.index
                             # if x != most_recent_row_index]
                    # print(f'RPMS export has duplicated rows for {measure}')
                    # subject_df.drop(non_recent_row_index, inplace=True)

        subject_df_dict[measure] = subject_df

    return subject_df_dict


@net.retry(max_attempts=5)
def sync(Lochness, subject, dry=False):
    logger.debug(f'exploring {subject.study}/{subject.id}')

    # for each subject
    subject_id = subject.id
    study_name = subject.study
    rpms_root_path = Lochness['RPMS_PATH']

    # source data
    all_df_dict = get_rpms_database(rpms_root_path)
    subject_df_dict = get_subject_data(all_df_dict,
                                       subject,
                                       Lochness['RPMS_id_colname'])

    for measure, source_df in subject_df_dict.items():
        if len(source_df) == 0:  # do not save if the dataframe is empty
            continue

        # target data
        dirname = tree.get('surveys',
                           subject.protected_folder,
                           processed=False,
                           BIDS=Lochness['BIDS'],
                           makedirs=True)
        target_df_loc = Path(dirname) / f"{subject_id}_{measure}.csv"

        proc_folder = tree.get('surveys',
                               subject.general_folder,
                               processed=True,
                               BIDS=Lochness['BIDS'],
                               makedirs=True)
        proc_dst = Path(proc_folder) / f"{subject_id}_{measure}.csv"

        # get_run_sheets_for_datatypes(target_df_loc)
        # if the csv already exists, compare the dataframe
        if Path(target_df_loc).is_file():
            # index might be different, so drop it before comparing it
            prev_df = pd.read_csv(target_df_loc,
                                  dtype=str).reset_index(drop=True)

            # source_df still has an index from the larger RPMS export
            # drop the index before the comparison to target_df
            same_df = source_df.reset_index(drop=True).equals(prev_df)
            if same_df:
                continue

        if not dry:
            Path(dirname).mkdir(exist_ok=True)
            #os.chmod(dirname, 0o0755)
            source_df.to_csv(target_df_loc, index=False)
            #os.chmod(target_df_loc, 0o0755)
            get_run_sheets_for_datatypes(target_df_loc)
            # process_and_copy_db(Lochness, subject, target_df_loc, proc_dst)


def update_study_metadata(subject, content: List[dict]) -> None:
    '''update metadata csv based on the rpms content: source_id'''

    sources = ['XNAT', 'Box', 'Mindlamp', 'Mediaflux', 'Daris']

    orig_metadata_df = pd.read_csv(subject.metadata_csv)

    subject_bool = orig_metadata_df['Subject ID'] == subject.id
    subject_index = orig_metadata_df[subject_bool].index
    subject_series = orig_metadata_df.loc[subject_index]
    other_metadata_df = orig_metadata_df[~subject_bool]

    updated = False
    for source in sources:
        if f"{source.lower()}_id" in content[0]:  # exist in the rpms
            source_id = content[0][f"{source.lower()}_id"]
            if source not in subject_series:
                subject_series[source] = f'{source.lower()}.{source_id}'
                updated = True

            # subject already has the information
            elif subject_series.iloc[0][source] != f'{source.lower()}.{source_id}':
                subject_series.iloc[0][source] = \
                        f'{source.lower()}.{source_id}'
                updated = True
            else:
                pass

    if updated:
        new_metadata_df = pd.concat([other_metadata_df, subject_series])

        # overwrite metadata
        new_metadata_df.to_csv(subject.metadata_csv, index=False)

