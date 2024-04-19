import lochness
import shutil
from lochness.box import sync, sync_module, get_access_token
from lochness import config
from pathlib import Path

import sys
lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))
from test_lochness import Args, KeyringAndEncrypt, Tokens
from test_lochness import show_tree_then_delete, rmtree, config_load_test
from test_lochness import initialize_metadata_test
from lochness_create_template import create_lochness_template
from boxsdk import Client, OAuth2

import pytest
import string
import requests
import time


box_test_dir = test_dir / 'lochness_test/box'
phoenix_root = box_test_dir / 'tmp_lochness/PHOENIX'
protected_root = phoenix_root/ 'PROTECTED'
general_root = phoenix_root/ 'GENERAL'


class KeyringAndEncrypt(KeyringAndEncrypt):
    def update_for_box(self, study):
        token = Tokens()
        client_id, client_secret, enterprise_id = \
                token.read_token_or_get_input('box')

        self.keyring[f'box.{study}'] = {}
        self.keyring[f'box.{study}']['CLIENT_ID'] = client_id
        self.keyring[f'box.{study}']['CLIENT_SECRET'] = client_secret
        self.keyring[f'box.{study}']['ENTERPRISE_ID'] = enterprise_id

        # get new api_token
        url = "https://api.box.com/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
                "box_subject_type": "enterprise",
                "box_subject_id": enterprise_id}

        # data = {"client_id": client_id,
                # "client_secret": client_secret,
                # "grant_type": "client_credentials",
                # "box_subject_type": "enterprise",
                # "box_subject_id": 799964836}

        response = requests.post(url, headers=headers, data=data)
        api_token = response.json()['access_token']
        self.keyring[f'box.{study}']['API_TOKEN'] = api_token

        self.write_keyring_and_encrypt()

@pytest.fixture
def args_and_Lochness():
    args = Args('tmp_lochness')
    args.studies = ['PronetLA', 'PronetAL']
    args.sources = ['box']
    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
    information_to_add_to_metadata = {'box': {
        'subject_id': '1001',
        'source_id': 'O1234'}}

    for study in args.studies:
        keyring.update_for_box(study)

        # update box metadata
        initialize_metadata_test('tmp_lochness/PHOENIX', study,
                                 information_to_add_to_metadata)

    lochness_obj = config_load_test('tmp_lochness/config.yml', '')

    return args, lochness_obj


@pytest.fixture
def args_and_Lochness_BIDS():
    args = Args('tmp_lochness')
    args.sources = ['box']
    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
    information_to_add_to_metadata = {'box': {
        'subject_id': '1001',
        'source_id': 'LA123456'}}

    for study in args.studies:
        keyring.update_for_box(study)

        # update box metadata
        initialize_metadata_test('tmp_lochness/PHOENIX', study,
                                 information_to_add_to_metadata)

    lochness_obj = config_load_test('tmp_lochness/config.yml', '')

    # change protect to true for all actigraphy
    for study in args.studies:
        # new_list = []
        lochness_obj['box'][study]['base'] = 'PronetLA'

    return args, lochness_obj


def test_box_client_connection_check():
    token = Tokens()
    client_id, client_secret, enterprise_id = \
            token.read_token_or_get_input('box')

    begin = time.time()
    api_token = get_access_token(client_id, client_secret, enterprise_id)

    # box authentication
    auth = OAuth2(
        client_id=client_id,
        client_secret=client_secret,
        access_token=api_token,
    )
    client = Client(auth)
    end = time.time()
    print(end - begin)

    begin = time.time()
    _ = client.user().get()
    end = time.time()
    print(end - begin)


def test_box_sync_module_default_nonBIDS(args_and_Lochness):
    args, Lochness = args_and_Lochness

    Lochness['BIDS'] = False
    # change protect to true for all actigraphy
    for study in args.studies:
        new_list = []
        Lochness['box'][study]['base'] = 'codmtg'
        for i in Lochness['box'][study]['file_patterns']['actigraphy']:
            i['protect'] = False
            i['processed'] = False
            new_list.append(i)
        Lochness['box'][study]['file_patterns']['actigraphy'] = new_list

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = general_root / study / '1001' / 'actigraphy' / 'raw'
        assert subject_dir.is_dir()
        assert len(list(subject_dir.glob('*csv'))) > 1

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_without_edits(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = protected_root / study / 'raw' / '1001' / 'actigraphy'
        assert subject_dir.is_dir()

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_paths_as_the_root_negative_1(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    for study in args.studies:
        # new_list = []
        Lochness['box'][study]['base'] = '/example_box_root/haha/PronetLA'

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = protected_root / study / 'raw' / '1001' / 'actigraphy'
        assert ~subject_dir.is_dir()

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_paths_as_the_root_negative_2(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    for study in args.studies:
        # new_list = []
        Lochness['box'][study]['base'] = '/example_box_root/PronetLA/haha'

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = protected_root / study / 'raw' / '1001' / 'actigraphy'
        assert ~subject_dir.is_dir()

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_paths_as_the_root_positive(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    for study in args.studies:
        # new_list = []
        Lochness['box'][study]['base'] = '/example_box_root/PronetLA'

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = protected_root / study / 'raw' / '1001' / 'actigraphy'
        assert subject_dir.is_dir()
        assert len(list(subject_dir.glob('*csv'))) == 1

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_default_BIDS(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    # change protect to true for all actigraphy
    for study in args.studies:
        new_list = []
        for i in Lochness['box'][study]['file_patterns']['actigraphy']:
            i['protect'] = False
            i['processed'] = False
            i['pattern'] = string.Template('*csv')
            new_list.append(i)
        Lochness['box'][study]['file_patterns']['actigraphy'] = new_list

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = general_root / study / 'raw' / '1001' / 'actigraphy'
        assert subject_dir.is_dir()
        assert len(list(subject_dir.glob('*csv'))) >= 1

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_protected(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    # change protect to true for all actigraphy
    for study in args.studies:
        new_list = []
        for i in Lochness['box'][study]['file_patterns']['actigraphy']:
            i['protect'] = True
            i['processed'] = False
            i['pattern'] = string.Template('*csv')
            new_list.append(i)
        Lochness['box'][study]['file_patterns']['actigraphy'] = new_list

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = protected_root / study / 'raw' / '1001' / 'actigraphy'
        print(subject_dir)
        assert subject_dir.is_dir()
        assert len(list(subject_dir.glob('*csv'))) == 1

        subject_dir = general_root / study / 'raw' / '1001' / 'actigraphy'
        assert subject_dir.is_dir() == False
        assert len(list(subject_dir.glob('*csv'))) == 0

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_protect_processed(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    # change protect to true for all actigraphy
    for study in args.studies:
        new_list = []
        for i in Lochness['box'][study]['file_patterns']['actigraphy']:
            i['protect'] = True
            i['processed'] = True
            i['pattern'] = string.Template('*csv')
            new_list.append(i)
        Lochness['box'][study]['file_patterns']['actigraphy'] = new_list

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    for study in args.studies:
        subject_dir = protected_root / study / 'processed' / '1001' / 'actigraphy'
        assert subject_dir.is_dir()
        assert len(list(subject_dir.glob('*csv'))) == 1

        subject_dir = general_root / study / 'processed' / '1001' / 'actigraphy'
        assert subject_dir.is_dir() == False
        assert len(list(subject_dir.glob('*csv'))) == 0

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_missing_root(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    # change base for StudyA to missing path
    Lochness['box']['StudyA']['base'] = 'hahah'

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    study = 'StudyA'
    subject_dir = protected_root / study / 'raw' / '1001' / 'actigraphy'
    assert subject_dir.is_dir() == False

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_missing_subject(args_and_Lochness):
    args, Lochness = args_and_Lochness

    # change subject name
    keyring = KeyringAndEncrypt(args.outdir)
    information_to_add_to_metadata = {'box': {
        'subject_id': '1001',
        'source_id': 'O12341234'}}

    for study in args.studies:
        keyring.update_for_box(study)

        # update box metadata
        initialize_metadata_test('tmp_lochness/PHOENIX', study,
                                 information_to_add_to_metadata)

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    show_tree_then_delete('tmp_lochness')


def test_box_sync_module_no_redownload(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS

    # change protect to true for all actigraphy
    for study in args.studies:
        new_list = []
        for i in Lochness['box'][study]['file_patterns']['actigraphy']:
            i['protect'] = True
            i['processed'] = False
            i['pattern'] = string.Template('*csv')
            new_list.append(i)
        Lochness['box'][study]['file_patterns']['actigraphy'] = new_list

    # change subject name
    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    a_file_path = protected_root / 'StudyA' / 'raw' / '1001' / 'actigraphy' / \
            'LA123456_actigraphy.csv'

    init_time = a_file_path.stat().st_mtime

    # change subject name
    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    post_time = a_file_path.stat().st_mtime
    assert init_time == post_time

    show_tree_then_delete('tmp_lochness')


def test_box_with_given_structure():
    site_subject = {
            'PronetLA': ['LA12345',
                         'LA13245',
                         'LA12435'],
            'PronetSL': ['SL12431',
                         'SL12348',
                         'SL12453'],
            'PronetWU': ['WU14235',
                         'WU12438',
                         'WU14253']
            }

    modalities = {
            'actigraphy': ['actigraphy'],
            'interviews': ['onsiteinterview', 'offsiteinterview'],
            'eeg': ['eeg']
            }

    # 'phone': ['accel', 'audioRecordings', 'gps', 'power', 'surveyAnswers', 'surveyTimings']


    root = Path('example_box_root')
    try:
        shutil.rmtree(root)
    except:
        pass

    for site, subjects in site_subject.items():

        site_dir = root / site
        for modality, subdirectories in modalities.items():
            modality_dir = site_dir / modality
            for subject in subjects:
                subject_dir = modality_dir / subject

                for file in subdirectories:
                    if modality == 'phone':
                        final_dir = modality_dir / file / subject
                        final_dir.mkdir(parents=True, exist_ok=True)

                        if file.endswith('interview') or file.endswith('cordings'):
                            suffix = 'mp4'
                        else:
                            suffix = 'csv'
                            
                        file_name = f"{subject}_{file}.{suffix}"

                        with open(final_dir / file_name, 'w') as f:
                            f.write('')
                    else:
                        final_dir = subject_dir
                        final_dir.mkdir(parents=True, exist_ok=True)

                        if file.endswith('interview') or file.endswith('cordings'):
                            suffix = 'mp4'
                        else:
                            suffix = 'csv'
                            
                        file_name = f"{subject}_{file}.{suffix}"

                        with open(final_dir / file_name, 'w') as f:
                            f.write('')

    # show_tree_then_delete('example_box_root')



def test_mediaflux_with_given_structure():
    site_subject = {
            'PrescientME': ['ME12345',
                            'ME13245',
                            'ME12435'],
            'PrescientAD': ['AD12430',
                            'AD12349',
                            'AD12453'],
            'PrescientPE': ['PE14235',
                            'PE12437',
                            'PE14253']
            }

    modalities = {
            'actigraphy': ['actigraphy'],
            'interviews': ['onsiteinterview', 'offsiteinterview'],
            'eeg': ['eeg']
            }

    # 'phone': ['accel', 'audioRecordings', 'gps', 'power', 'surveyAnswers', 'surveyTimings']


    root = Path('example_mediaflux_root')
    try:
        shutil.rmtree(root)
    except:
        pass

    for site, subjects in site_subject.items():

        site_dir = root / site
        for modality, subdirectories in modalities.items():
            modality_dir = site_dir / modality
            for subject in subjects:
                subject_dir = modality_dir / subject

                for file in subdirectories:
                    if modality == 'phone':
                        final_dir = modality_dir / file / subject
                        final_dir.mkdir(parents=True, exist_ok=True)

                        if file.endswith('interview') or file.endswith('cordings'):
                            suffix = 'mp4'
                        else:
                            suffix = 'csv'
                            
                        file_name = f"{subject}_{file}.{suffix}"

                        with open(final_dir / file_name, 'w') as f:
                            f.write('')
                    else:
                        final_dir = subject_dir
                        final_dir.mkdir(parents=True, exist_ok=True)

                        if file.endswith('interview') or file.endswith('cordings'):
                            suffix = 'mp4'
                        else:
                            suffix = 'csv'
                            
                        file_name = f"{subject}_{file}.{suffix}"

                        with open(final_dir / file_name, 'w') as f:
                            f.write('')



def test_box_sync_Phil(args_and_Lochness):
    '''Test pulling data keeping the structure of the subdirectories in box'''
    args, Lochness = args_and_Lochness

    # change subject name LA49125 test subject - this data is uploaded to Box
    # for test
    keyring = KeyringAndEncrypt(args.outdir)
    information_to_add_to_metadata = {'box':{
        'subject_id': 'LA49125',
        'source_id': 'LA49125'}
        }

    for study in args.studies:
        keyring.update_for_box(study)

        # update box metadata
        initialize_metadata_test('tmp_lochness/PHOENIX', study,
                                 information_to_add_to_metadata)

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

    # show_tree_then_delete('tmp_lochness')


def test_box_sync_issue_Aug():
    '''Test pulling data keeping the structure of the subdirectories in box'''
    args = Args('tmp_lochness')
    args.studies = ['PronetKC', 'PronetKC', 'PronetPA']
    args.sources = ['box']
    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
        # {'subject_id': 'PV06517',
        # 'source_id': 'PV06517'},
    information_to_add_to_metadata = {'box':
            [
        {'subject_id': 'PA14348',
        'source_id': 'PA14348'}
        ]
        }

    for study in args.studies:
        keyring.update_for_box(study)

        # update box metadata
        initialize_metadata_test('tmp_lochness/PHOENIX', study,
                                 information_to_add_to_metadata)

    Lochness = config_load_test('tmp_lochness/config.yml', '')

    for subject in lochness.read_phoenix_metadata(Lochness):
        sync(Lochness, subject, dry=False)

