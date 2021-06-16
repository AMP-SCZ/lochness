import lochness
from lochness.daris import daris_download, collect_all_daris_metadata, sync
import lochness.config as config

from pathlib import Path
import sys

import pytest
import shutil
import os

import zipfile
import tempfile as tf
import json
import cryptease as crypt
import pandas as pd

lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))
from lochness_create_template import create_lochness_template

from test_lochness import Args, Tokens, KeyringAndEncrypt, args, Lochness
from test_lochness import show_tree_then_delete, config_load_test


class Args(Args):
    def __init__(self, root_dir):
        super().__init__(root_dir)
        self.studies = ['StudyA']
        self.sources = ['Redcap', 'xNat', 'boX', 'daris']
        self.poll_interval = 10


@pytest.fixture
def args():
    return Args('test_lochness')


class KeyringAndEncryptDaris(KeyringAndEncrypt):
    def __init__(self, tmp_dir):
        super().__init__(tmp_dir)
        token = Tokens()
        token, url, project_cid = token.read_token_or_get_input('daris')

        self.keyring['daris.StudyA']['TOKEN'] = token
        self.keyring['daris.StudyA']['URL'] = url
        self.keyring['daris.StudyA']['PROJECT_CID'] = project_cid

        self.write_keyring_and_encrypt()


@pytest.fixture
def Lochness():
    args = Args('tmp_lochness')
    create_lochness_template(args)
    KeyringAndEncryptDaris(args.outdir)

    lochness = config_load_test('tmp_lochness/config.yml', '')
    return lochness


def test_daris_download():
    daris_uid = 'subject01'
    latest_pull_mtime = 0
    token = Tokens()
    token, url, project_cid = token.read_token_or_get_input('daris')
    dst_zipfile = 'tmp.zip'

    daris_download(daris_uid, latest_pull_mtime, token,
                   project_cid, url, dst_zipfile)

    tmpdir = tf.mkdtemp(dir='.', prefix='.')
    with zipfile.ZipFile(dst_zipfile, 'r') as zip_ref:
        zip_ref.extractall(tmpdir)

    nfiles_in_dirs = []
    for root, dirs, files in os.walk(tmpdir):
        for directory in dirs:
            os.chmod(os.path.join(root, directory), 0o0755)
        for f in files:
            os.chmod(os.path.join(root, f), 0o0755)
        nfiles_in_dirs.append(len(files))

    # if there is any new file downloaded save timestamp
    if any([x > 1 for x in nfiles_in_dirs]):
        print('Downloaded')
    else:
        print('No file downloaded')

    os.remove(dst_zipfile)
    shutil.rmtree(tmpdir)


def initialize_metadata(Lochness, study_name):
    df = pd.DataFrame({'Active': [1],
        'Consent': '1988-09-16',
        'Subject ID': 'subject01',
        'Daris': 'daris.StudyA:5Yp0E'})
    df_loc = Path(Lochness['phoenix_root']) / 'GENERAL' / \
            study_name / f"{study_name}_metadata.csv"

    df.to_csv(df_loc, index=False)


def test_sync_from_empty(Lochness):
    dry=False
    study_name = 'StudyA'
    initialize_metadata(Lochness, study_name)

    for subject in lochness.read_phoenix_metadata(Lochness,
                                                  studies=[study_name]):
        sync(Lochness, subject, dry)

    show_tree_then_delete('tmp_lochness')


