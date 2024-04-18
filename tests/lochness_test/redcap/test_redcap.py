import lochness
import pandas as pd
import json, os, time
from lochness import tree
from pathlib import Path

from lochness.redcap import sync, initialize_metadata
from lochness.redcap import get_run_sheets_for_datatypes, post_to_redcap, \
        deidentify_flag, iterate, redcap_projects
from lochness.redcap.data_trigger_capture import save_post_from_redcap
from lochness.redcap.data_trigger_capture import back_up_db

import sys
lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))
from test_lochness import Args, KeyringAndEncrypt
from test_lochness import show_tree_then_delete, rmtree, config_load_test
from lochness_create_template import create_lochness_template

import pytest
pd.set_option('display.max_columns', 50)


@pytest.fixture
def args_and_Lochness():
    args = Args('tmp_lochness')
    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
    for study in args.studies:
        keyring.update_for_redcap(study)

    lochness_obj = config_load_test('tmp_lochness/config.yml', '')

    return args, lochness_obj



@pytest.fixture
def args_and_Lochness_ampsz():
    args = Args('tmp_lochness')
    args.studies = ['ProNET_BA', 'Pronet_LA']
    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
    for study in args.studies:
        keyring.update_for_ampsz_redcap(study)

    lochness_obj = config_load_test('tmp_lochness/config.yml', '')

    return args, lochness_obj


@pytest.fixture
def args_and_Lochness_fake():
    args = Args('tmp_lochness')
    args.studies = ['FAKE_AD', 'FAKE_LA']
    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
    for study in args.studies:
        keyring.update_for_fake_redcap(study)

    lochness_obj = config_load_test('tmp_lochness/config.yml', '')

    return args, lochness_obj


def test_initialize_metadata_fake_then_sync(args_and_Lochness_fake):
    args, Lochness = args_and_Lochness_fake

    # before initializing metadata
    for study in args.studies:
        phoenix_path = Path(Lochness['phoenix_root'])
        general_path = phoenix_path / 'GENERAL'
        initialize_metadata(Lochness, study,
                'chric_subject_id',
                'chric_consent_date')

    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=args.studies):
        sync(Lochness, subject, False)

    # show_tree_then_delete('tmp_lochness')

def test_initialize_metadata_ampsz_then_sync(args_and_Lochness_ampsz):
    args, Lochness = args_and_Lochness_ampsz

    # before initializing metadata
    for study in args.studies:
        phoenix_path = Path(Lochness['phoenix_root'])
        general_path = phoenix_path / 'GENERAL'
        initialize_metadata(Lochness, study, 'chric_subject_id',
                'chric_consent_date')

    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=args.studies):
        sync(Lochness, subject, False)

    show_tree_then_delete('tmp_lochness')


def test_initialize_metadata_function_adding_new_data_to_csv(
        args_and_Lochness):
    args, Lochness = args_and_Lochness

    # before initializing metadata
    for study in args.studies:
        phoenix_path = Path(Lochness['phoenix_root'])
        general_path = phoenix_path / 'GENERAL'
        protected_path = phoenix_path / 'PROTECTED'
        metadata = protected_path / study / f"{study}_metadata.csv"
        assert len(pd.read_csv(metadata)) == 1

        initialize_metadata(Lochness, study, 'record_id1', 'cons_date', False)

        assert len(pd.read_csv(metadata)) > 3

    rmtree('tmp_lochness')


def test_initialize_metadata_then_sync(args_and_Lochness):
    args, Lochness = args_and_Lochness

    # before initializing metadata
    for study in args.studies:
        phoenix_path = Path(Lochness['phoenix_root'])
        general_path = phoenix_path / 'GENERAL'
        protected_path = phoenix_path / 'PROTECTED'
        metadata = protected_path / study / f"{study}_metadata.csv"
        initialize_metadata(Lochness, study, 'record_id1', 'cons_date', False)

    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=['StudyA']):
        sync(Lochness, subject, False)

    show_tree_then_delete('tmp_lochness')


@pytest.fixture
def LochnessMetadataInitialized(args_and_Lochness):
    args, Lochness = args_and_Lochness

    # before initializing metadata
    for study in args.studies:
        phoenix_path = Path(Lochness['phoenix_root'])
        general_path = phoenix_path / 'GENERAL'
        protected_path = phoenix_path / 'PROTECTED'
        metadata = protected_path / study / f"{study}_metadata.csv"

        initialize_metadata(Lochness, study, 'record_id1', 'cons_date', False)

    return Lochness


def test_initialize_metadata_update_when_initialized_again(
        LochnessMetadataInitialized):
    args = Args('tmp_lochness')
    for study in args.studies:
        phoenix_path = Path(LochnessMetadataInitialized['phoenix_root'])
        general_path = phoenix_path / 'GENERAL'
        protected_path = phoenix_path / 'PROTECTED'
        metadata = protected_path / study / f"{study}_metadata.csv"

        prev_st_mtime = metadata.stat().st_mtime

        initialize_metadata(LochnessMetadataInitialized, study,
                            'record_id1', 'cons_date', False)
        post_st_mtime = metadata.stat().st_mtime

        assert prev_st_mtime < post_st_mtime


@pytest.fixture
def lochness_subject_raw_json(LochnessMetadataInitialized):
    for subject in lochness.read_phoenix_metadata(LochnessMetadataInitialized,
                                                  studies=['StudyA']):
        if subject.id != 'subject_1':
            continue

        phoenix_path = Path(LochnessMetadataInitialized['phoenix_root'])
        subject_proc_p = phoenix_path / 'PROTECTED' / 'StudyA' / 'raw' / 'surveys' / 'subject_1'
        raw_json = subject_proc_p / f"{subject.id}.StudyA.json"

        return LochnessMetadataInitialized, subject, raw_json


def test_sync_init(lochness_subject_raw_json):
    Lochness, subject, raw_json = lochness_subject_raw_json
    sync(Lochness, subject, False)
    assert raw_json.is_file() == True
    rmtree('tmp_lochness')


def test_sync_twice(lochness_subject_raw_json):
    Lochness, subject, raw_json = lochness_subject_raw_json
    sync(Lochness, subject, False)
    # second sync without update in the db
    sync(Lochness, subject, False)
    rmtree('tmp_lochness')


def test_sync_no_mtime_update_when_no_pull(
        lochness_subject_raw_json):
    Lochness, subject, raw_json = lochness_subject_raw_json
    sync(Lochness, subject, False)
    init_mtime = raw_json.stat().st_mtime

    # second sync without update in the db
    sync(Lochness, subject, False)
    assert init_mtime == raw_json.stat().st_mtime
    rmtree('tmp_lochness')


def test_sync_det_update_no_file_leads_to_pull(
        lochness_subject_raw_json):
    Lochness, subject, raw_json = lochness_subject_raw_json
    sync(Lochness, subject, False)
    init_mtime = raw_json.stat().st_mtime

    os.remove(raw_json)
    text_body = "redcap_url=https%3A%2F%2Fredcap.partners.org%2Fredcap%2F&project_url=https%3A%2F%2Fredcap.partners.org%2Fredcap%2Fredcap_v10.0.30%2Findex.php%3Fpid%3D26709&project_id=26709&username=kc244&record=subject_1&instrument=inclusionexclusion_checklist&inclusionexclusion_checklist_complete=0"
    save_post_from_redcap(
            text_body,
            Lochness['redcap']['data_entry_trigger_csv'])

    # second sync without update in the db
    sync(Lochness, subject, False)
    assert raw_json.is_file()
    rmtree('tmp_lochness')


def test_sync_det_update_while_file_leads_to_mtime_update(
        lochness_subject_raw_json):
    Lochness, subject, raw_json = lochness_subject_raw_json
    sync(Lochness, subject, False)
    init_mtime = raw_json.stat().st_mtime

    text_body = "redcap_url=https%3A%2F%2Fredcap.partners.org%2Fredcap%2F&project_url=https%3A%2F%2Fredcap.partners.org%2Fredcap%2Fredcap_v10.0.30%2Findex.php%3Fpid%3D26709&project_id=26709&username=kc244&record=subject_1&instrument=inclusionexclusion_checklist&inclusionexclusion_checklist_complete=0"
    save_post_from_redcap(
            text_body,
            Lochness['redcap']['data_entry_trigger_csv'])

    with open(raw_json, 'r') as json_file:
        init_content_dict = json.load(json_file)

    # second sync without update in the db
    sync(Lochness, subject, False)

    with open(raw_json, 'r') as json_file:
        new_content_dict = json.load(json_file)

    assert init_content_dict == new_content_dict
    assert init_mtime < raw_json.stat().st_mtime
    rmtree('tmp_lochness')


def test_sync_det_update_while_diff_file_leads_to_data_overwrite(
        lochness_subject_raw_json):
    Lochness, subject, raw_json = lochness_subject_raw_json
    print(Lochness)

    sync(Lochness, subject, False)

    # change the content of the existing json
    with open(raw_json, 'w') as json_file:
        json.dump({'test': 'test'}, json_file)

    text_body = "redcap_url=https%3A%2F%2Fredcap.partners.org%2Fredcap%2F&project_url=https%3A%2F%2Fredcap.partners.org%2Fredcap%2Fredcap_v10.0.30%2Findex.php%3Fpid%3D26709&project_id=26709&username=kc244&record=subject_1&instrument=inclusionexclusion_checklist&inclusionexclusion_checklist_complete=0"
    save_post_from_redcap(
            text_body,
            Lochness['redcap']['StudyA']['data_entry_trigger_csv'])

    # second sync without update in the db
    sync(Lochness, subject, False)

    with open(raw_json, 'r') as json_file:
        new_content_dict = json.load(json_file)
    assert {'test': 'test'} != new_content_dict
    rmtree('tmp_lochness')


def test_get_run_sheet_for_datatypes():
    from lochness.config import load
    config_loc = '/opt/software/Pronet_data_sync/config.yml'
    Lochness = load(config_loc)

    # get URL
    keyring = Lochness['keyring']
    api_url = keyring['redcap.Pronet']['URL'] + '/api/'
    api_key = keyring['redcap.Pronet']['API_TOKEN']['Pronet']
    id_field = Lochness['redcap_id_colname']
    for subject_path in (
            Path(Lochness['phoenix_root']) / 'PROTECTED').glob('*/raw/*'):
        subject = subject_path.name
        print(subject)
        site = subject[:2]
        json_path = subject_path / f'surveys/{subject}.Pronet.json'
        print(json_path)
        get_run_sheets_for_datatypes(
                api_url, api_key, subject, id_field, json_path)
        return


def test_deidentification():
    from lochness.config import load
    config_loc = '/opt/software/Pronet_data_sync/config.yml'
    Lochness = load(config_loc)

    # get URL
    keyring = Lochness['keyring']
    api_url = keyring['redcap.Pronet']['URL'] + '/api/'
    api_key = keyring['redcap.Pronet']['API_TOKEN']['Pronet']
    id_field = Lochness['redcap_id_colname']
    for subject_path in (
            Path(Lochness['phoenix_root']) / 'PROTECTED').glob('*/raw/*'):
        subject = subject_path.name
        print(subject)
        site = subject[:2]
        json_path = subject_path / f'surveys/{subject}.Pronet.json'
        redcap_subject = subject
        redcap_subject_sl = redcap_subject.lower()
        record_query = {
            'token': api_key,
            'content': 'record',
            'format': 'json',
            'filterLogic': f"[{id_field}] = '{redcap_subject}' or "
                           f"[{id_field}] = '{redcap_subject_sl}'"
           }
        content = post_to_redcap(api_url, record_query, '')
        content_dict_list = json.loads(content)

        for content_dict in content_dict_list:
            deidentify = deidentify_flag(Lochness, 'PronetLA')
            # deidentify = False
            if deidentify:
                metadata_query = {
                    'token': api_key,
                    'content': 'metadata',
                    'format': 'json',
                    'returnFormat': 'json'
                }

                content = post_to_redcap(api_url, metadata_query, '')
                metadata = json.loads(content)
                field_names = []
                field_num = 0
                non_field_num = 0
                total_num = 0
                for field in metadata:
                    if field['identifier'] == 'y':
                        try:
                            content_dict.pop(field['field_name'])
                        except:
                            print(field['field_name'])

        test = str(content_dict_list).encode('utf-8')
        lochness.atomic_write('test.json', test)
        return


def test_upenn_longitudinal_data():
    '''Check if the redcap module successively pulls longitudinal data'''
    from lochness.config import load
    config_loc = '/mnt/ProNET/Lochness/config.yml'
    Lochness = load(config_loc)

    # get URL
    keyring = Lochness['keyring']
    api_url = keyring['redcap.UPENN']['URL'] + '/api/'
    api_key = keyring['redcap.UPENN']['API_TOKEN']['UPENN']
    id_field = Lochness['redcap_id_colname']
    for subject_path in (
            Path(Lochness['phoenix_root']) / 'PROTECTED').glob('*/raw/*'):
        subject = subject_path.name
        if 'YA015' in subject:
            break
    redcap_subject = subject
    redcap_subject_sl = redcap_subject.lower()
    record_query = {
        'token': api_key,
        'content': 'record',
        'format': 'json',
        'filterLogic': f"[session_subid] = '{redcap_subject}' or "
                       f"[session_subid] = '{redcap_subject_sl}'"
    }

    content = post_to_redcap(api_url,
                             record_query,
                             '')
    content_dict_list = json.loads(content)


    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=['PronetYA']):
        if 'YA015' in subject.id:
            sync(Lochness, subject, False)


def test_upenn_inclusive_id():
    from lochness.config import load
    config_loc = '/mnt/ProNET/Lochness/config.yml'
    Lochness = load(config_loc)

    # get URL
    keyring = Lochness['keyring']
    api_url = keyring['redcap.UPENN']['URL'] + '/api/'
    api_key = keyring['redcap.UPENN']['API_TOKEN']['UPENN']
    id_field = Lochness['redcap_id_colname']
    for subject_path in (
            Path(Lochness['phoenix_root']) / 'PROTECTED').glob('*/raw/*'):
        subject = subject_path.name
        if 'YA05293' in subject:
            break
    redcap_subject = subject
    redcap_subject_sl = redcap_subject.lower()
    # digits = [1, 2]
    digits = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    digits_str = [str(x) for x in digits]
    contains_logic = []
    for subject_id in [redcap_subject, redcap_subject_sl]:
        contains_logic += [
                f"contains([session_subid], '{subject_id}_{x}')"
                for x in digits_str]
        contains_logic += [
                f"contains([session_subid], '{subject_id}={x}')"
                for x in digits_str]


    record_query = {
        'token': api_key,
        'content': 'record',
        'format': 'json',
        'filterLogic': f"[session_subid] = '{redcap_subject}' or "
                       f"[session_subid] = '{redcap_subject_sl}' or "
                       f"{' or '.join(contains_logic)}"
    }
    print()
    # print(record_query)
    print()

    content = post_to_redcap(api_url,
                             record_query,
                             '')
    content_dict_list = json.loads(content)
    print(len(content_dict_list))
    assert len(content_dict_list) == 5
