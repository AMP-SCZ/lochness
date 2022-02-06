import lochness.mindlamp
from lochness.keyring import pretty_print_dict
from lochness import config
import os
from pathlib import Path
import pandas as pd
pd.set_option('max_columns', 50)
pd.set_option('max_rows', 500)

import sys
lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))

from test_lochness import Args, Tokens, KeyringAndEncrypt, args, SyncArgs
from test_lochness import show_tree_then_delete, config_load_test
from test_lochness import initialize_metadata_test
from lochness_create_template import create_lochness_template

from sync import do

# from config.test_config import create_config
import LAMP
from typing import Tuple, List
from lochness.mindlamp import get_study_lamp, get_participants_lamp
from lochness.mindlamp import get_activities_lamp, get_sensors_lamp
from lochness.mindlamp import get_activity_events_lamp
from lochness.mindlamp import get_sensor_events_lamp
from lochness.mindlamp import sync, get_days_to_pull
import json
import base64
import time
from datetime import datetime, timedelta
import pytz

import pytest


class KeyringAndEncryptMindlamp(KeyringAndEncrypt):
    def __init__(self, tmp_dir):
        super().__init__(tmp_dir)
        token = Tokens()
        mindlamp_token, access_key, secret_key, api_url = \
                token.read_token_or_get_input('mindlamp')
                # token.get_mindlamp_token()

        self.keyring['mindlamp.StudyA'] = {}
        self.keyring['mindlamp.StudyA']['ACCESS_KEY'] = access_key
        self.keyring['mindlamp.StudyA']['SECRET_KEY'] = secret_key
        self.keyring['mindlamp.StudyA']['URL'] = api_url

        self.write_keyring_and_encrypt()


class KeyringAndEncryptMindlampOrygen(KeyringAndEncrypt):
    def __init__(self, tmp_dir):
        super().__init__(tmp_dir)
        token = Tokens()
        mindlamp_token, access_key, secret_key, api_url = \
                token.read_token_or_get_input('mindlamp_orygen')
                # token.get_mindlamp_token()

        self.keyring['mindlamp.StudyA'] = {}
        self.keyring['mindlamp.StudyA']['ACCESS_KEY'] = access_key
        self.keyring['mindlamp.StudyA']['SECRET_KEY'] = secret_key
        self.keyring['mindlamp.StudyA']['URL'] = api_url

        self.write_keyring_and_encrypt()


class KeyringAndEncryptMindlampAdmin(KeyringAndEncrypt):
    def __init__(self, tmp_dir):
        super().__init__(tmp_dir)
        token = Tokens()
        mindlamp_token, access_key, secret_key, api_url = \
                token.read_token_or_get_input('mindlamp_admin')
                # token.get_mindlamp_token()

        self.keyring['mindlamp.StudyA'] = {}
        self.keyring['mindlamp.StudyA']['ACCESS_KEY'] = access_key
        self.keyring['mindlamp.StudyA']['SECRET_KEY'] = secret_key
        self.keyring['mindlamp.StudyA']['URL'] = api_url

        self.write_keyring_and_encrypt()


class KeyringAndEncryptMindlampAdminIP(KeyringAndEncrypt):
    def __init__(self, tmp_dir):
        super().__init__(tmp_dir)
        token = Tokens()
        mindlamp_token, access_key, secret_key, api_url = \
                token.read_token_or_get_input('mindlamp_admin_ip')
                # token.get_mindlamp_token()

        self.keyring['mindlamp.StudyA'] = {}
        self.keyring['mindlamp.StudyA']['ACCESS_KEY'] = access_key
        self.keyring['mindlamp.StudyA']['SECRET_KEY'] = secret_key
        self.keyring['mindlamp.StudyA']['URL'] = api_url

        self.write_keyring_and_encrypt()


class KeyringAndEncryptMindlampYoon(KeyringAndEncrypt):
    def __init__(self, tmp_dir):
        super().__init__(tmp_dir)
        token = Tokens()
        mindlamp_token, access_key, secret_key, api_url = \
                token.read_token_or_get_input('mindlamp_yoon')
                # token.get_mindlamp_token()

        self.keyring['mindlamp.StudyA'] = {}
        self.keyring['mindlamp.StudyA']['ACCESS_KEY'] = access_key
        self.keyring['mindlamp.StudyA']['SECRET_KEY'] = secret_key
        self.keyring['mindlamp.StudyA']['URL'] = api_url

        self.write_keyring_and_encrypt()

@pytest.fixture
def get_lamp():
    token = Tokens()
    mindlamp_token, access_key, secret_key, api_url = \
            token.read_token_or_get_input('mindlamp')
    LAMP.connect(access_key, secret_key)

    return LAMP


@pytest.fixture
def get_lamp_orygen():
    token = Tokens()
    mindlamp_token, access_key, secret_key, api_url = \
            token.read_token_or_get_input('mindlamp_admin')
    
    LAMP.connect(access_key, secret_key, api_url)

    return LAMP


def test_lamp_download_speed(get_lamp):
    LAMP = get_lamp

    subject_id = 'U6198539134'  # Yoon's data

    # activity data
    start = time.time()
    _ = LAMP.ActivityEvent.all_by_participant(
            subject_id)
    end = time.time()
    time_to_pull_all_data = end - start

    start = time.time()
    _ = LAMP.ActivityEvent.all_by_participant(
            subject_id, _from="1626806920000", to="1627578306405")
    end = time.time()
    time_to_pull_partial_data = end - start

    assert time_to_pull_all_data > time_to_pull_partial_data, \
            'It took more time to pull partial activity data than ' \
            'the full data'

    # sensor data
    start = time.time()
    _ = LAMP.SensorEvent.all_by_participant(
            subject_id)
    end = time.time()
    time_to_pull_all_data = end - start

    start = time.time()
    _ = LAMP.SensorEvent.all_by_participant(
            subject_id, _from="1626806920000", to="1627578306405")
    end = time.time()
    time_to_pull_partial_data = end - start

    assert time_to_pull_all_data > time_to_pull_partial_data, \
            'It took more time to pull partial sensor data than ' \
            'the full data'


def test_lamp_get_studyname(get_lamp):
    LAMP = get_lamp
    study_id, study_name = get_study_lamp(LAMP)
    print(study_id, study_name)

    subject_ids = get_participants_lamp(LAMP, study_id)
    print(subject_ids)


def test_lamp_example_data(get_lamp):
    LAMP = get_lamp
    subject_ids = ['U4518928219', 'U6198539134']

    for subject_id in subject_ids:
        activity_dicts = get_activity_events_lamp(LAMP, subject_id)
        sensor_dicts = get_sensor_events_lamp(LAMP, subject_id)

        with open(f'{subject_id}_activity_data.json', 'w') as f:
            json.dump(activity_dicts, f)

        with open(f'{subject_id}_sensor_data.json', 'w') as f:
            json.dump(sensor_dicts, f)

        with open(f'{subject_id}_activity_data.json', 'r') as f:
            pretty_print_dict(json.load(f))

        with open(f'{subject_id}_sensor_data.json', 'r') as f:
            pretty_print_dict(json.load(f))

        os.remove(f'{subject_id}_sensor_data.json')
        os.remove(f'{subject_id}_activity_data.json')


def test_sync_mindlamp(args):
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    _ = KeyringAndEncryptMindlamp(args.outdir)


    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': {'subject_id': '1001',
                                                   'source_id': 'U7045332804'}}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)

    show_tree_then_delete('tmp_lochness')


def test_sound_download(get_lamp):
    LAMP = get_lamp

    subject_id = 'U6198539134'  # Yoon's data

    activity_events_dicts = LAMP.ActivityEvent.all_by_participant(
            subject_id, _from="1631730650831", to="1631730650833")

    audio = activity_events_dicts['data'][0]['static_data']['url']
    decode_bytes = base64.b64decode(audio.split(',')[1])

    with open('test.mp3', 'wb') as f:
        f.write(decode_bytes)


def test_timestamp_to_UTC_0_24():
    timestamp1 = time.mktime(datetime.now().timetuple())
    print(timestamp1)
    print(datetime.utcfromtimestamp(timestamp1))

    # current time in UTC
    ct_utc = datetime.now(pytz.timezone('UTC'))

    # 
    ct_utc_00 = ct_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    ct_utc_00 = ct_utc_00 - timedelta(days=1)
    ct_utc_00_ts = time.mktime(ct_utc_00.timetuple()) * 1000
    ct_utc_24 = ct_utc_00 + timedelta(hours=24)
    ct_utc_24_ts = time.mktime(ct_utc_24.timetuple()) * 1000

    print(ct_utc_24.strftime('%Y_%m_%d'))


@pytest.fixture
def args_and_Lochness():
    args = Args('tmp_lochness')
    args.studies = ['PronetLA', 'PronetSL']

    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
    for study in args.studies:
        # for pronet & prescient
        project_name = study[:-2]
        keyring.update_for_redcap(project_name)

    lochness_obj = config_load_test('tmp_lochness/config.yml', '')

    return args, lochness_obj


def test_get_days_to_pull(args_and_Lochness):
    args, Lochness = args_and_Lochness
    Lochness['mindlamp_days_to_pull'] = None
    days_to_pull = get_days_to_pull(Lochness)
    assert days_to_pull == 100

    Lochness['mindlamp_days_to_pull'] = 5
    days_to_pull = get_days_to_pull(Lochness)
    assert days_to_pull == 5


def test_sync_mindlamp(args):
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    _ = KeyringAndEncryptMindlamp(args.outdir)

    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': {'subject_id': '1001',
                                                   'source_id': 'U6198539134'}}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)

    show_tree_then_delete('tmp_lochness')


def test_sync_mindlamp_orygen_Yoon(args):
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    _ = KeyringAndEncryptMindlampOrygen(args.outdir)

    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': {'subject_id': '1001',
                                                   'source_id': 'U2862696942'}}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)

    show_tree_then_delete('tmp_lochness')


def test_sync_mindlamp_orygen_admin(args):
    # for mindlamp_id in ['U2862696942', 'U5891709819']:
    # args.outdir = 'tmp_lochness'
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    _ = KeyringAndEncryptMindlampAdmin(args.outdir)
    # _ = KeyringAndEncryptMindlampYoon(args.outdir)

    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': {'subject_id': '1001',
                                                   'source_id': 'U2862696942'}}
                                                   # 'source_id': 'U5891709819'}}
                                                   # 'source_id': 'U3025891176'}}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)



def test_pulling_timestamp_from_to(get_lamp_orygen):
    LAMP = get_lamp_orygen
    subject_ids = ['U2862696942', 'U5891709819']
    subject_id = subject_ids[0]
    ts_set = 1636347600192
    data_dict = get_sensor_events_lamp(
            LAMP, subject_id,
            from_ts=ts_set, to_ts=ts_set)[0]

    # print(data_dict[0])
    test_json_loc = '/Users/kc244/lochness/tests/lochness_test/mindlamp/tmp_lochness/PHOENIX/PROTECTED/StudyA/raw/1001/phone/U2862696942_StudyA_sensor_2021_11_08.json'
    with open(test_json_loc, 'r') as fp:
        new_data = json.load(fp)[-1]

    print()
    # print(new_data)

    j1 = json.dumps(data_dict, sort_keys=True)
    j2 = json.dumps(new_data, sort_keys=True)

    print(j1)
    print(j2)
    print(j1 == j2)

    # for subject_id in subject_ids:
        # activity_dicts = get_activity_events_lamp(LAMP, subject_id)
        # sensor_dicts = get_sensor_events_lamp(LAMP, subject_id)

        # with open(f'{subject_id}_activity_data.json', 'w') as f:
            # json.dump(activity_dicts, f)

        # with open(f'{subject_id}_sensor_data.json', 'w') as f:
            # json.dump(sensor_dicts, f)

        # with open(f'{subject_id}_activity_data.json', 'r') as f:
            # pretty_print_dict(json.load(f))

        # with open(f'{subject_id}_sensor_data.json', 'r') as f:
            # pretty_print_dict(json.load(f))

        # os.remove(f'{subject_id}_sensor_data.json')
        # os.remove(f'{subject_id}_activity_data.json')


def test_sync_skip_for_the_day(args):
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    _ = KeyringAndEncryptMindlampAdmin(args.outdir)
    # _ = KeyringAndEncryptMindlampYoon(args.outdir)

    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': [
        {'subject_id': '1001', 'source_id': 'U2862696942'},
        {'subject_id': '1002', 'source_id': 'U5891709819'}
        ]}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)


def test_sync_skip_for_the_day_ip(args):
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    # _ = KeyringAndEncryptMindlampAdminIP(args.outdir)
    _ = KeyringAndEncryptMindlampAdmin(args.outdir)
    # _ = KeyringAndEncryptMindlampYoon(args.outdir)

    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': [
        {'subject_id': '1001', 'source_id': 'U2862696942'},
        {'subject_id': '1002', 'source_id': 'U5891709819'}
        ]}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)


def test_pronet_mindlamp(args):
    args.studies = ['StudyA']
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    # _ = KeyringAndEncryptMindlampAdminIP(args.outdir)
    # _ = KeyringAndEncryptMindlampAdmin(args.outdir)
    _ = KeyringAndEncryptMindlamp(args.outdir)
    # _ = KeyringAndEncryptMindlampYoon(args.outdir)

    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': [
        {'subject_id': '1001', 'source_id': 'U5891709819'},
        ]}
        # {'subject_id': '1001', 'source_id': 'U2763080389'},
        # {'subject_id': '1002', 'source_id': 'U4361826716'}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)

    Lochness['mindlamp_days_to_pull'] = 3
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)


def test_sync_yoon_and_habib_feb_2022(args):
    syncArgs = SyncArgs(args.outdir)
    syncArgs.studies = ['StudyA']
    sources = ['mindlamp']
    syncArgs.update_source(sources)

    create_lochness_template(args)
    syncArgs.config = args.outdir / 'config.yml'
    # _ = KeyringAndEncryptMindlampAdminIP(args.outdir)
    _ = KeyringAndEncryptMindlampAdmin(args.outdir)
    # _ = KeyringAndEncryptMindlampYoon(args.outdir)

    phoenix_root = args.outdir / 'PHOENIX'
    information_to_add_to_metadata = {'mindlamp': [
        {'subject_id': '1003', 'source_id': 'U5891709819'},
        {'subject_id': '1004', 'source_id': 'U2862696942'}
        ]}
    
    initialize_metadata_test(phoenix_root, 'StudyA',
                             information_to_add_to_metadata)
    Lochness = config_load_test(syncArgs.config)
    Lochness['mindlamp_days_to_pull'] = 8
    for subject in lochness.read_phoenix_metadata(Lochness, syncArgs.studies):
        sync(Lochness, subject, False)
