import lochness
import os
import shutil
from lochness.transfer import get_updated_files, compress_list_of_files
from lochness.transfer import compress_new_files, lochness_to_lochness_transfer
from pathlib import Path
import sys
scripts_dir = Path(lochness.__path__[0]).parent / 'scripts'
sys.path.append(str(scripts_dir))
from lochness_create_template import create_lochness_template

import lochness.config as config
import pytest
from time import time
from datetime import timedelta
from datetime import datetime
import json
import cryptease as crypt

class Args:
    def __init__(self, root_dir):
        self.outdir = root_dir
        self.studies = ['StudyA', 'StudyB']
        self.sources = ['Redcap', 'RPMS']
        self.poll_interval = 10
        self.ssh_user = 'kc244'
        self.ssh_host = 'erisone.partners.org'
        self.email = 'kevincho@bwh.harvard.edu'
        self.lochness_sync_history_csv = 'lochness_sync_history.csv'
        self.det_csv = 'prac.csv'
        self.pii_csv = ''


@pytest.fixture
def args():
    return Args('test_lochness')


def test_get_updated_files(args):
    print()

    timestamp_a_day_ago = datetime.timestamp(
            datetime.fromtimestamp(time()) - timedelta(days=1))

    outdir = 'tmp_lochness'
    args.outdir = outdir
    create_lochness_template(args)
    posttime = time()

    file_lists = get_updated_files(Path(args.outdir / 'PHOENIX'),
                                   timestamp_a_day_ago,
                                   posttime)

    assert Path('PHOENIX/GENERAL/StudyA/StudyA_metadata.csv') in file_lists
    assert Path('PHOENIX/GENERAL/StudyB/StudyB_metadata.csv') in file_lists

    shutil.rmtree('tmp_lochness')

    print(file_lists)
    print(file_lists)



def test_compress_list_of_files(args):
    print()

    timestamp_a_day_ago = datetime.timestamp(
            datetime.fromtimestamp(time()) - timedelta(days=1))

    outdir = 'tmp_lochness'
    args.outdir = outdir
    create_lochness_template(args)
    posttime = time()

    phoenix_root = Path(args.outdir / 'PHOENIX')
    file_lists = get_updated_files(phoenix_root,
                                   timestamp_a_day_ago,
                                   posttime)
    compress_list_of_files(phoenix_root, file_lists, 'prac.tar')

    shutil.rmtree('tmp_lochness')

    # shutil.rmtree('tmp_lochness')
    assert Path('prac.tar').is_file()

    os.popen('tar -xf prac.tar').read()
    print(os.popen('tree').read())
    shutil.rmtree('PHOENIX')
    os.remove('prac.tar')


def test_compress_new_files(args):
    print()
    outdir = 'tmp_lochness'
    args.outdir = outdir
    create_lochness_template(args)

    phoenix_root = Path(args.outdir / 'PHOENIX')

    compress_new_files('nodb', phoenix_root, 'prac.tar')
    shutil.rmtree('tmp_lochness')

    # shutil.rmtree('tmp_lochness')
    assert Path('prac.tar').is_file()
    assert Path('nodb').is_file()

    with open('nodb', 'r') as f:
        print(f.read())

    os.popen('tar -xf prac.tar').read()
    os.remove('nodb')
    os.remove('prac.tar')
    print(os.popen('tree').read())
    shutil.rmtree('PHOENIX')


def update_keyring_and_encrypt(tmp_lochness_dir: str):
    keyring_loc = Path(tmp_lochness_dir) / 'lochness.json'
    with open(keyring_loc, 'r') as f:
        keyring = json.load(f)

    keyring['rpms.StudyA']['RPMS_PATH'] = str(
            Path(tmp_lochness_dir).absolute().parent / 'RPMS_repo')

    with open(keyring_loc, 'w') as f:
        json.dump(keyring, f)
    
    keyring_content = open(keyring_loc, 'rb')
    key = crypt.kdf('')
    crypt.encrypt(keyring_content, key,
            filename=Path(tmp_lochness_dir) / '.lochness.enc')


def test_lochness_to_lochness_transfer(args):
    print()
    outdir = 'tmp_lochness'
    args.outdir = outdir
    create_lochness_template(args)
    update_keyring_and_encrypt(args.outdir)

    Lochness = config.load('tmp_lochness/config.yml', '')

    lochness_to_lochness_transfer(Lochness)
    print(os.popen('tree').read())
    shutil.rmtree('tmp_lochness')
    os.remove('lochness_sync_history.csv')
    compressed_file = list(Path('.').glob('tmp*tar'))[0]
    os.popen(f'tar -xf {compressed_file}').read()
    os.remove(str(compressed_file))
    print(os.popen('tree').read())
    shutil.rmtree('PHOENIX')