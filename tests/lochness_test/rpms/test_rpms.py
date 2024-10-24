import lochness
import sys
from pathlib import Path
import lochness.tree as tree
import time
import pandas as pd

lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))

from lochness_create_template import create_lochness_template
from test_lochness import Tokens, KeyringAndEncrypt, args, Lochness
from test_lochness import show_tree_then_delete, config_load_test
from lochness.rpms import initialize_metadata, sync, get_rpms_database
from lochness.rpms import get_run_sheets_for_datatypes, get_subject_data

import pytest


def create_fake_rpms_repo():
    '''Create fake RPMS repo per variable'''
    # make REPO directory
    root = Path('RPMS_repo')
    root.mkdir(exist_ok=True)

    number_of_measures = 10
    number_of_subjects = 5
    for measure_num in range(0, number_of_measures):
        # create a data
        measure_file = root / f'PrescientStudy_Prescient_{measure_num}' \
                              '_01.01.1988.csv'

        df = pd.DataFrame()

        for subject_num in range(0, number_of_subjects):
            for site in 'AD', 'DA':
                df_tmp = pd.DataFrame({
                    'subjectkey': [f'{site}{subject_num}'],
                    'Consent': '1988-09-16',
                    'var1': f'{measure_num}_var1_subject_{subject_num}',
                    'address': f'{measure_num}_var2_subject_{subject_num}',
                    'var3': f'{measure_num}_var3_subject_{subject_num}',
                    'xnat_id': f'StudyA:bwh:var3_subject_{subject_num}',
                    'box_id': f'box.StudyA:var3_subject_{subject_num}',
                    'mindlamp_id': f'box.StudyA:var3_subject_{subject_num}',
                    'LastModifiedDate': time.time()})
                if measure_num // 2 >= 1:
                    df_tmp = df_tmp.drop('box_id', axis=1)
                else:
                    df_tmp = df_tmp.drop('xnat_id', axis=1)

                df = pd.concat([df, df_tmp])

        df.to_csv(measure_file, index=False)


def test_initializing_based_on_rpms(Lochness):
    '''Test updating the metadata

    Current model
    =============

    - RPMS_PATH
        - subject01
          - subject01.csv
        - subject02
          - subject02.csv
        - subject03
          - subject03.csv
        - ...
    '''
    create_fake_rpms_repo()
    Lochness['RPMS_PATH'] = Path('RPMS_repo').absolute()
    initialize_metadata(Lochness, 'StudyA', 'subjectkey', 'Consent', False)
    df = pd.read_csv('tmp_lochness/PHOENIX/PROTECTED/StudyA/StudyA_metadata.csv')
    print(df)
    show_tree_then_delete('tmp_lochness')
    assert len(df) == 10


def test_create_lochness_template(Lochness):
    create_fake_rpms_repo()
    # create_lochness_template(args)
    study_name = 'StudyA'
    Lochness['RPMS_PATH'] = Path('RPMS_repo').absolute()
    initialize_metadata(Lochness, study_name, 'subjectkey', 'Consent', False)

    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=['StudyA']):
        # print(subject)
        for module in subject.rpms:
            print(module)
            print(module)
            print(module)
            # break
        # break

    show_tree_then_delete('tmp_lochness')


def test_sync(Lochness):
    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=['StudyA']):
        sync(Lochness, subject, False)


class KeyringAndEncryptRPMS(KeyringAndEncrypt):
    def __init__(self, tmp_lochness_dir):
        super().__init__(tmp_lochness_dir)
        self.keyring['rpms.StudyA']['RPMS_PATH'] = str(
                Path(self.tmp_lochness_dir).absolute().parent / 'RPMS_repo')
        print(self.keyring)

        self.write_keyring_and_encrypt()


def test_sync_from_empty(args):
    outdir = 'tmp_lochness'
    args.outdir = outdir
    args.sources = ['RPMS']
    args.studies = ['PrescientAD', 'PrescientDA']
    create_lochness_template(args)
    KeyringAndEncrypt(args.outdir)
    create_fake_rpms_repo()

    dry=False
    study_name = 'PrescientAD'
    Lochness = config_load_test(f'{args.outdir}/config.yml', '')
    Lochness['RPMS_PATH'] = str(
            Path(outdir).absolute().parent / 'RPMS_repo')
    initialize_metadata(Lochness, study_name, Lochness['RPMS_id_colname'],
            'Consent', True)

    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=[study_name]):
        sync(Lochness, subject, dry)

    # print the structure
    show_tree_then_delete('tmp_lochness')


# rpms_root_path: str
def test_get_rpms_database():
    rpms_root_path = 'RPMS_repo'
    all_df_dict = get_rpms_database(rpms_root_path)

    assert list(all_df_dict.keys())[0]=='measure_0'
    assert list(all_df_dict.keys())[1]=='measure_8'

    assert type(list(all_df_dict.values())[0]) == pd.core.frame.DataFrame


def test_get_rpms_real_example(args):
    outdir = 'tmp_lochness'
    args.outdir = outdir
    args.sources = ['RPMS']
    create_lochness_template(args)
    create_fake_rpms_repo()
    KeyringAndEncrypt(Path(outdir))

    dry=False
    study_name = 'StudyA'
    Lochness = config_load_test(f'{args.outdir}/config.yml', '')

    initialize_metadata(Lochness, study_name, 'src_subject_id', 'Consent', False)

    for subject in lochness.read_phoenix_metadata(
            Lochness, studies=['StudyA']):
        sync(Lochness, subject, dry)

    # print the structure
    show_tree_then_delete('tmp_lochness')

def test_get_rpms_real_example_sync(args):
    outdir = 'tmp_lochness'
    args.outdir = outdir
    args.s3 = True
    args.sources = ['RPMS']
    args.BIDS = True
    create_lochness_template(args)
    create_fake_rpms_repo()
    KeyringAndEncrypt(Path(outdir))

    dry=False
    study_name = 'StudyA'
    Lochness = config_load_test(f'{args.outdir}/config.yml', '')
    print(Lochness)
    print(Lochness['BIDS'])
    print(Lochness['BIDS'])
    print(Lochness['BIDS'])
    # # Lochness['keyring']['rpms.StudyA']['RPMS_PATH'] = '/mnt/prescient/RPMS_incoming'

    initialize_metadata(
            Lochness, study_name, 
            Lochness['RPMS_id_colname'], 'Consent', False)

    for subject in lochness.read_phoenix_metadata(
            Lochness, studies=['StudyA']):
        sync(Lochness, subject, dry)


def test_get_runsheets_for_datatypes():
    outdir = 'tmp_lochness_2'
    args.outdir = outdir
    args.s3 = True
    args.sources = ['RPMS']
    args.BIDS = True
    args.studies = ['PrescientME', 'PrescientDA']
    args.det_csv = 'det_test.csv'
    args.pii_csv = 'pii_test.csv'
    args.lochness_sync_history_csv = 'lochness_sync_history_test.csv'
    args.poll_interval = 1234
    args.ssh_user = 1234
    args.ssh_host = 1234
    args.s3_selective_sync = False
    args.email = 'kevincho@bwh.harvard.edu'
    args.lochness_sync_send = False
    args.lochness_sync_receive = False
    create_lochness_template(args)
    create_fake_rpms_repo()
    KeyringAndEncrypt(Path(outdir))

    dry=False
    study_name = 'PrescientME'
    Lochness = config_load_test(f'{args.outdir}/config.yml', '')
    print(Lochness)
    print(Lochness['BIDS'])
    print(Lochness['BIDS'])
    print(Lochness['BIDS'])
    # # Lochness['keyring']['rpms.StudyA']['RPMS_PATH'] = '/mnt/prescient/RPMS_incoming'

    initialize_metadata(
            Lochness,
            study_name, 
            Lochness['RPMS_id_colname'],
            'Consent', False)

    for subject in lochness.read_phoenix_metadata(
            Lochness, studies=['PrescientME']):
        sync(Lochness, subject, dry)

    show_tree_then_delete('tmp_lochness_2')


def test_loading_db():
    rpms_root_path = '/mnt/prescient/RPMS_incoming'
    db_dict = get_rpms_database(rpms_root_path)


def test_repeated_measure_first():
    rpms_root_path = '/mnt/prescient/RPMS_incoming'
    db_dict = get_rpms_database(rpms_root_path)
    example_repeated_keyname = 'past_pharmaceutical_treatment'
    example_repeated_keyname = 'chrdbb_lamp_id'
    print(db_dict[example_repeated_keyname])


def test_repeated_measure_second():
    rpms_root_path = '/mnt/prescient/RPMS_incoming'
    all_df_dict = get_rpms_database(rpms_root_path)
    example_repeated_keyname = 'past_pharmaceutical_treatment'
    dup_id = 'ME08857'

    df_measure_all_subj = all_df_dict['informed_consent_run_sheet']
    for subject, table in df_measure_all_subj.groupby('subjectkey'):
        table = table[['subjectkey',
            'chric_consent_date', 'LastModifiedDate']].drop_duplicates()

        if len(table) > 1:
            table['LastModifiedDate'] = pd.to_datetime(
                    table['LastModifiedDate'])
            table = table.sort_values('LastModifiedDate')
            print(table)
            print(table.iloc[-1])
    return
    for measure, df_measure_all_subj in all_df_dict.items():
        # if not measure == example_repeated_keyname:
            # continue

        # loop through each line of the RPMS database
        # for index, df_measure in df_measure_all_subj.iterrows():
        for subject, df_measure in df_measure_all_subj.groupby('subjectkey'):
            if len(df_measure) > 1:
                print(df_measure)
                break
                # return

            # return
            # if 'chrdbb_lamp_id' in df_measure:
                # print(df_measure_all_subj)
                # return
                # print(df_measure['chrdbb_lamp_id'])
            # if df_measure.subjectkey == dup_id:
                # print(df_measure)



def test_gets_subject_data():
    class SubjectTest(object):
        pass
    rpms_root_path = '/mnt/prescient/RPMS_incoming'
    all_df_dict = get_rpms_database(rpms_root_path)
    subject = SubjectTest()
    subject.id = 'TE00001'
    id_colname = 'subjectkey'
    subject_df_dict = get_subject_data(all_df_dict, subject, id_colname)
    for key, table in subject_df_dict.items():
        print(key, len(table))


def test_subjects_with_multiple_consent_dates():
    Lochness = {}
    Lochness['RPMS_PATH'] = '/mnt/prescient/RPMS_incoming'
    Lochness['phoenix_root'] = 'test'
    study_name = 'GW'
    rpms_consent_colname = 'chric_consent_date'
    rpms_id_colname = 'subjectkey'
    Lochness['RPMS_id_colname'] = rpms_id_colname
    out_metadata = Path(f'test/PROTECTED/{study_name}/{study_name}_metadata.csv')
    out_metadata.parent.mkdir(exist_ok=True)

    initialize_metadata(Lochness,
                        study_name,
                        rpms_id_colname,
                        rpms_consent_colname)
    df = pd.read_csv(out_metadata)
    with open('tmp_subject_id_to_check.txt', 'r') as fp:
        subject_id = fp.read().strip()
    print(df[df['Subject ID']==subject_id]['Consent'])


def test_get_run_sheets_for_datatypes():
    class SubjectObj():
        def __init__(self, id):
            self.id = id

    subject_id = 'ME50349'
    rpms_root_path = Path('/var/lib/prescient/RPMS_incoming')
    all_df_dict = get_rpms_database(rpms_root_path)
    subject_df_dict = get_subject_data(all_df_dict,
                                       SubjectObj(subject_id),
                                       'subjectkey')
    for measure, source_df in subject_df_dict.items():
        target_df_loc = Path('/var/lib/prescient/data/PHOENIX/PROTECTED/PrescientME/raw/ME50349/surveys/ME50349_eeg_run_sheet.csv')
        target_df_loc.parent.mkdir(exist_ok=True, parents=True)
        get_run_sheets_for_datatypes(target_df_loc)
