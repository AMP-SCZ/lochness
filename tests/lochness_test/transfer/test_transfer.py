import lochness
import os
import shutil
from datetime import datetime
from lochness.config import load
from lochness.transfer import get_updated_files, compress_list_of_files
from lochness.transfer import get_updated_files, compress_list_of_files
from lochness.transfer import compress_new_files
from lochness.transfer import lochness_to_lochness_transfer_sftp
from lochness.transfer import decompress_transferred_file_and_copy
from lochness.transfer import lochness_to_lochness_transfer_receive_sftp
from lochness.transfer import lochness_to_lochness_transfer_s3
from lochness.transfer import lochness_to_lochness_transfer_s3_protected
from lochness.transfer import create_s3_transfer_table
from lochness.transfer import send_file_to_s3_phoenix

from pathlib import Path
import re

import sys
lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))

from test_lochness import Args, Tokens, KeyringAndEncrypt, args, SyncArgs
from test_lochness import show_tree_then_delete, config_load_test

from lochness_create_template import create_lochness_template


import pytest
from time import time
from datetime import timedelta
from datetime import datetime
import json
import pandas as pd
import cryptease as crypt
import tempfile as tf
import tarfile
import paramiko

from sync import do


@pytest.fixture
def Lochness():
    args = Args('tmp_lochness')
    args.rsync = True
    create_lochness_template(args)
    k = KeyringAndEncrypt(args.outdir)
    k.update_var_subvars('lochness_sync', 'transfer')

    lochness = config_load_test('tmp_lochness/config.yml', '')
    return lochness


def test_get_updated_files(Lochness):

    timestamp_a_day_ago = datetime.timestamp(
            datetime.fromtimestamp(time()) - timedelta(days=1))

    posttime = time()

    file_lists = get_updated_files(Lochness['phoenix_root'],
                                   timestamp_a_day_ago,
                                   posttime)

    assert Path('PHOENIX/PROTECTED/StudyA/StudyA_metadata.csv') in file_lists
    assert Path('PHOENIX/PROTECTED/StudyB/StudyB_metadata.csv') in file_lists

    show_tree_then_delete('tmp_lochness')


def test_compress_list_of_files(Lochness):
    print()

    timestamp_a_day_ago = datetime.timestamp(
            datetime.fromtimestamp(time()) - timedelta(days=1))

    posttime = time()

    phoenix_root = Lochness['phoenix_root']
    file_lists = get_updated_files(phoenix_root,
                                   timestamp_a_day_ago,
                                   posttime)
    compress_list_of_files(phoenix_root, file_lists, 'prac.tar')

    show_tree_then_delete('tmp_lochness')

    # shutil.rmtree('tmp_lochness')
    assert Path('prac.tar').is_file()

    os.popen('tar -xf prac.tar').read()
    show_tree_then_delete('PHOENIX')
    os.remove('prac.tar')


def test_compress_new_files(Lochness):
    print()

    phoenix_root = Lochness['phoenix_root']

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

    show_tree_then_delete('PHOENIX')



def test_lochness_to_lochness_transfer_all(Lochness):
    print()

    protected_dir = Path(Lochness['phoenix_root']) / 'PROTECTED'

    for i in range(10):
        with tf.NamedTemporaryFile(suffix='tmp.text',
                                   delete=False,
                                   dir=protected_dir) as tmpfilename:

            with open(tmpfilename.name, 'w') as f:
                f.write('ha')


    #pull all
    # lochness_to_lochness_transfer(Lochness, general_only=False)

    with tf.NamedTemporaryFile(suffix='tmp.tar',
                               delete=False,
                               dir='.') as tmpfilename:
        # compress
        compress_new_files(Lochness['lochness_sync_history_csv'],
                           Lochness['phoenix_root'],
                           tmpfilename.name,
                           False)

    show_tree_then_delete('tmp_lochness')

    compressed_file = list(Path('.').glob('tmp*tar'))[0]
    os.popen(f'tar -xf {compressed_file}').read()
    os.remove(str(compressed_file))

    show_tree_then_delete('PHOENIX')


def test_decompress_transferred_file_and_copy():
    target_phoenix_root = Path('DPACC_PHOENIX')

    tar_file_trasferred = list(Path('.').glob('tmp*tar'))[0]
    decompress_transferred_file_and_copy(target_phoenix_root,
                                         tar_file_trasferred)

    show_tree_then_delete(target_phoenix_root)


def test_lochness_to_lochness_transfer_receive(Lochness):
    print()

    protected_dir = Path(Lochness['phoenix_root']) / 'PROTECTED'

    for i in range(10):
        with tf.NamedTemporaryFile(suffix='tmp.text',
                                   delete=False,
                                   dir=protected_dir) as tmpfilename:

            with open(tmpfilename.name, 'w') as f:
                f.write('ha')


    #pull all
    # lochness_to_lochness_transfer(Lochness, general_only=False)

    with tf.NamedTemporaryFile(suffix='tmp.tar',
                               delete=False,
                               dir='.') as tmpfilename:
        # compress
        compress_new_files(Lochness['lochness_sync_history_csv'],
                           Lochness['phoenix_root'],
                           tmpfilename.name,
                           False)

    show_tree_then_delete('tmp_lochness')

    compressed_file = list(Path('.').glob('tmp*tar'))[0]
    os.popen(f'tar -xf {compressed_file}').read()
    os.remove(str(compressed_file))

    show_tree_then_delete('PHOENIX')


    out_dir = 'DPACC'
    args = Args(out_dir)
    create_lochness_template(args)
    update_keyring_and_encrypt_DPACC(args.outdir)

    lochness = config_load_test(f'{out_dir}/config.yml', '')
    lochness_to_lochness_transfer_receive_sftp(lochness)


    show_tree_then_delete('DPACC')


def update_keyring_and_encrypt_DPACC(tmp_lochness_dir: str):
    keyring_loc = Path(tmp_lochness_dir) / 'lochness.json'
    with open(keyring_loc, 'r') as f:
        keyring = json.load(f)

    keyring['lochness_sync']['PATH_IN_HOST'] = '.'

    with open(keyring_loc, 'w') as f:
        json.dump(keyring, f)
    
    keyring_content = open(keyring_loc, 'rb')
    key = crypt.kdf('')
    crypt.encrypt(keyring_content, key,
                  filename=Path(tmp_lochness_dir) / '.lochness.enc')


def test_lochness_to_lochness_transfer(Lochness):
    print()
    protected_dir = Path(Lochness['phoenix_root']) / 'PROTECTED'

    for i in range(10):
        with tf.NamedTemporaryFile(suffix='tmp.text',
                                   delete=False,
                                   dir=protected_dir) as tmpfilename:

            with open(tmpfilename.name, 'w') as f:
                f.write('ha')


    lochness_to_lochness_transfer_sftp(Lochness, False)
    print(os.popen('tree').read())
    shutil.rmtree('tmp_lochness')

    compressed_file = list(Path('.').glob('tmp*tar'))[0]
    os.popen(f'tar -xf {compressed_file}').read()
    os.remove(str(compressed_file))

    show_tree_then_delete('PHOENIX')



def test_sftp():
    tokens = Tokens()
    host, username, password, path_in_host, port = \
            tokens.read_token_or_get_input('transfer')
    file_to_send = 'hoho.txt'
    with open(file_to_send, 'w') as f:
        f.write('hahahah')

    transport = paramiko.Transport((host, int(port)))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    sftp.put(file_to_send, str(Path(path_in_host) / Path(file_to_send).name))
    sftp.close()
    transport.close()

    os.remove('hoho.txt')


def test_using_sync_do_send(Lochness):
    syncArg = SyncArgs('tmp_lochness')
    syncArg.lochness_sync_send = True
    syncArg.input_sources = syncArg.source
    do(syncArg)
    show_tree_then_delete('tmp_lochness')


@pytest.fixture
def LochnessRsync():
    args = Args('tmp_lochness')
    args.rsync = True
    create_lochness_template(args)
    k = KeyringAndEncrypt(args.outdir)
    k.update_var_subvars('rsync', 'rsync')

    lochness = config_load_test('tmp_lochness/config.yml', '')
    return lochness


@pytest.fixture
def LochnessS3():
    args = Args('tmp_lochness')
    args.s3 = True
    create_lochness_template(args)
    keyringObj = KeyringAndEncrypt(args.outdir)

    lochness = config_load_test('tmp_lochness/config.yml', '')
    return lochness


def test_lochness_to_lochness_transfer_rsync(LochnessRsync):
    syncArg = SyncArgs('tmp_lochness')
    syncArg.lochness_sync_send = True
    syncArg.rsync = True
    syncArg.input_sources = syncArg.source
    do(syncArg)

    show_tree_then_delete('tmp_lochness')


def test_lochness_to_lochness_transfer_s3(LochnessS3):
    syncArg = SyncArgs('tmp_lochness')
    syncArg.lochness_sync_send = True
    syncArg.s3 = True
    syncArg.input_sources = syncArg.source
    do(syncArg)

    command = 's3-tree ampscz-dev'
    print(os.popen(command).read())

    command = 'aws s3 rm s3://ampscz-dev/TEST_PHOENIX_ROOT --recursive'
    print(os.popen(command).read())

    show_tree_then_delete('tmp_lochness')


def test_s3_sync_function(Lochness):
    '''Test below requires s3-tree and aws, with bucket called ampscz-dev'''
    Lochness['AWS_BUCKET_NAME'] = 'ampscz-dev'
    Lochness['AWS_BUCKET_ROOT'] = 'TEST_PHOENIX_ROOT'
    lochness_to_lochness_transfer_s3(Lochness, True)

    command = 's3-tree ampscz-dev'
    print(os.popen(command).read())

    command = 'aws s3 rm s3://ampscz-dev/TEST_PHOENIX_ROOT --recursive'
    print(os.popen(command).read())

    
def test_lochness_to_lochness_transfer_s3_protected():
    args = Args('tmp_lochness')
    args.rsync = True
    args.s3_selective_sync = ['actigraphy', 'haha']
    create_lochness_template(args)
    k = KeyringAndEncrypt(args.outdir)
    k.update_var_subvars('lochness_sync', 'transfer')

    lochness = config_load_test('tmp_lochness/config.yml', '')
    print(lochness)
    # print(lochness)

    lochness['AWS_BUCKET_NAME'] = 'ampscz-dev'
    lochness['AWS_BUCKET_ROOT'] = 'TEST_PHOENIX_ROOT'

    # create fake files
    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/raw/subject01/actigraphy/haha.txt'
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file.touch()

    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/raw/subject01/actigraphy/haha2.txt'
    tmp_file.touch()

    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/raw/subject02/actigraphy/haha2.txt'
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file.touch()

    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/processed/subject02/actigraphy/haha2.txt'
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file.touch()

    # Lochness['AWS_BUCKET_NAME'] = 'ampscz-dev'
    # Lochness['AWS_BUCKET_ROOT'] = 'TEST_PHOENIX_ROOT'
    lochness_to_lochness_transfer_s3_protected(
            lochness, lochness['s3_selective_sync'])


def test_create_s3_transfer_table():
    import re
    log_file = 'log.txt'

    if Path('ha.csv').is_file():
        df_prev = pd.read_csv('ha.csv', index_col=0)
        most_recent_time_stamp_prev_run = pd.to_datetime(df_prev['timestamp']).max()
    else:
        df_prev = pd.DataFrame()
        most_recent_time_stamp_prev_run = pd.to_datetime('2000-01-01')

    df = pd.DataFrame()
    with open(log_file, 'r') as f:
        for line in f.readlines():
            if 'lochness.transfer' in line:
                most_recent_time_stamp = re.search(r'^(\S+ \w+:\w+:\w+)',
                                                   line).group(1)
                most_recent_time_stamp = pd.to_datetime(most_recent_time_stamp)
                more_recent = most_recent_time_stamp > most_recent_time_stamp_prev_run

            if line.startswith('upload: '):
                if not more_recent:
                    continue

                try:
                    source = re.search(r'upload: (\S+)', line).group(1)
                    target = re.search(r'upload: (\S+) to (\S+)',
                                       line).group(2)

                    df_tmp = pd.DataFrame({
                        'timestamp': [most_recent_time_stamp],
                        'source': Path(source),
                        'destination': Path(target)})

                    df = pd.concat([df, df_tmp])
                except AttributeError:
                    pass

    if len(df) == 0:
        print('No new data')
        return

    # register datatypes, study and subject
    df['filename'] = df['source'].apply(lambda x: x.name)
    df['protected'] = df['source'].apply(lambda x: x.parts[1])
    df['study'] = df['source'].apply(lambda x: x.parts[2])
    df['processed'] = df['source'].apply(lambda x: x.parts[3])
    df['subject'] = df['source'].apply(lambda x: x.parts[4]
            if len(x.parts) > 4 else '')
    df['datatypes'] = df['source'].apply(lambda x: x.parts[5]
            if len(x.parts) > 5 else '')

    df = df.reset_index()
    df.loc[df[df['filename'].str.contains('metadata.csv')].index, 'processed'] = ''
    df.timestamp = pd.to_datetime(df.timestamp)
    
    df = pd.concat([df_prev, df.drop('index', axis=1)])

    print(df)
    df.to_csv('ha.csv')


def test_aws_s3_no_sync_when_no_data():
    args = Args('tmp_lochness')
    args.rsync = True
    args.s3_selective_sync = ['actigraphy']
    create_lochness_template(args)
    k = KeyringAndEncrypt(args.outdir)
    k.update_var_subvars('lochness_sync', 'transfer')

    lochness = config_load_test('tmp_lochness/config.yml', '')
    # print(lochness)

    # create fake files
    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/raw/subject01/actigraphy/haha.txt'
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    # if not tmp_file.is_file():
    tmp_file.touch()

    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/raw/subject01/actigraphy/haha2.txt'
    # if not tmp_file.is_file():
    tmp_file.touch()

    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/raw/subject02/actigraphy/haha2.txt'
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    # if not tmp_file.is_file():
    tmp_file.touch()

    tmp_file = Path(lochness['phoenix_root']) / \
            'PROTECTED/StudyA/processed/subject02/actigraphy/haha2.txt'
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    # if not tmp_file.is_file():
    tmp_file.touch()

    lochness['AWS_BUCKET_NAME'] = 'prod-ampscz-pronet'
    lochness['AWS_BUCKET_ROOT'] = 'TEST_aws'
    lochness_to_lochness_transfer_s3_protected(lochness)

    print('second upload')
    # s3_log = Path(lochness['phoenix_root']) / 'aws_s3_sync_stdouts.log'
    # with open(s3_log, 'a') as fp:
        # fp.write('second run\n')

    lochness_to_lochness_transfer_s3_protected(lochness)
    print('completed')


    # print('permission change')
    os.chmod(tmp_file, 0o777)
    # os.chown(tmp_file, 0, 1004)
    lochness_to_lochness_transfer_s3_protected(lochness)
    print('completed')


def test_aws_s3_sync_pure():
    temp_dir = Path('tmp_pure_dir')
    temp_dir.mkdir(exist_ok=True)

    temp_file_1 = temp_dir / 'test1.csv'
    temp_file_2 = temp_dir / 'test2.csv'

    for i in temp_file_1, temp_file_2:
        if not i.is_file():
            i.touch()

    command = f'aws s3 sync {temp_dir} s3://prod-ampscz-pronet/TEST_aws2'
    print(command)

    print(os.popen(command).read())


def test_create_s3_transfer_table():
    args = Args('tmp_lochness')
    args.rsync = True
    args.s3_selective_sync = ['actigraphy']
    create_lochness_template(args)
    k = KeyringAndEncrypt(args.outdir)
    k.update_var_subvars('lochness_sync', 'transfer')

    Lochness = config_load_test('tmp_lochness/config.yml', '')
    create_s3_transfer_table(Lochness)


def test_create_s3_transfer_table_real():
    Lochness = load('/mnt/ProNET/Lochness/config.yml')
    create_s3_transfer_table(Lochness)


def test_parallel_transfer():
    Lochness = load('/mnt/ProNET/Lochness/config.yml')
    # lochness_to_lochness_transfer_s3_protected(
            # Lochness,
            # ['PronetYA', 'PronetCA'],
            # sources=['redcap', 'box', 'mindlamp'])
    lochness_to_lochness_transfer_s3(
            Lochness,
            ['PronetYA', 'PronetCA'],
            sources=['box'])

    # create_s3_transfer_table(Lochness)


def test_create_s3_transfer_table_with_prev_data():
    args = Args('tmp_lochness')
    args.rsync = True
    args.s3_selective_sync = ['actigraphy']
    create_lochness_template(args)
    k = KeyringAndEncrypt(args.outdir)
    k.update_var_subvars('lochness_sync', 'transfer')

    lochness = config_load_test('tmp_lochness/config.yml', '')
    shutil.copy(
        '/mnt/ProNET/Lochness/PHOENIX/aws_s3_sync_stdouts.log',
        lochness['phoenix_root'])

    log_file = Path(lochness['phoenix_root']) / 'aws_s3_sync_stdouts.log'
    out_file = Path(lochness['phoenix_root']) / 's3_log.csv'

    max_ts_prev_df = pd.to_datetime('2000-01-01')
    df = pd.DataFrame()
    with open(log_file, 'r') as fp:
        for line in fp.readlines():
            if 'LA00145' not in line:
                continue

            if not 'upload' in line:
                continue

            try:
                re_line = re.search(r'^(\S+ \w+:\w+:\w+) upload: (\S+)', line)
                ts = pd.to_datetime(re_line.group(1))
                print(line)
                print(len(line))
                # if len(line) > 500:
                    # print(line)
                    # return
            except AttributeError:
                print(line)
                return
                continue

            more_recent = ts > max_ts_prev_df
            if not more_recent:
                continue

            source = re_line.group(2)

            # make source relative to PHOENIX root parent
            if not source.startswith('PHOENIX'):
                source = 'PHOENIX/' + source.split('/PHOENIX/')[1]

            source = Path(source)
            # do not save metadata.csv update since it
            # gets updated every pull
            if 'metadata.csv' in source.name:
                continue

            target = re.search(r'upload: (\S+) to (\S+)',
                               line).group(2)

            df_tmp = pd.DataFrame({'timestamp': [ts],
                                   'source': source,
                                   'destination': Path(target)})

            df = pd.concat([df, df_tmp])

    print(df)


def test_send_file_to_s3_phoenix():
    print()
    Lochness = {'AWS_BUCKET_NAME': 'test',
                'AWS_BUCKET_ROOT': 'test-phoenix',
                'phoenix_root': '/data/predict/PHOENIX'}

    prac_file = Path(Lochness['phoenix_root']) / \
            'GENERAL/PronetAB/raw/AB00000/survey/hoho.csv'
    send_file_to_s3_phoenix(Lochness, prac_file)

