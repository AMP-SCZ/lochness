from typing import List
from pathlib import Path
from datetime import datetime
import os
import pandas as pd
import subprocess
from time import time
import tempfile as tf
import sys
import paramiko

from typing import List


def get_updated_files(phoenix_root: str,
                      timestamp_start: int,
                      timestamp_end: int,
                      general_only: bool = True) -> List[Path]:
    '''Return list of file paths updated betwee time window using GNU find

    In order to compress the new files with the original structure from the
    PHOENIX root, the absolute paths returned by GNU find is changed to 
    relative path from the PHOENIX root.

    Key arguments:
        phoenix_root: PHOENIX root, str.
        timestamp_start: start of the GNU find search window in timestamp, int.
        timestamp_end: end of the GNU find search window in timestamp, int.
        general_only: only searches new files under GENERAL directory, bool.

    Returns:
        file_paths_relative: relative paths of new files, list of Path objects.

    '''
    date_time_start = datetime.fromtimestamp(timestamp_start).replace(
            microsecond=0)
    date_time_end = datetime.fromtimestamp(timestamp_end).replace(
            microsecond=0)

    if general_only:
        find_command = f'find {phoenix_root} ' \
                       f'-path {phoenix_root}/PROTECTED -prune -o ' \
                       f'\( -type f ' \
                       f'-newermt "{date_time_start}" ' \
                       f'! -newermt "{date_time_end}" \)'
    else:
        find_command = f'find {phoenix_root} ' \
                       f'-type f ' \
                       f'-newermt "{date_time_start}" ' \
                       f'! -newermt "{date_time_end}"'

    proc = subprocess.Popen(find_command, shell=True, stdout=subprocess.PIPE)
    proc.wait()

    outs, _ = proc.communicate()

    file_paths_absolute = outs.decode().strip().split('\n')
    file_paths_relative = [Path(x).relative_to(Path(phoenix_root).parent) for x
                           in file_paths_absolute]

    if general_only:
        # remove the PROTECTED DIR
        file_paths_relative = [x for x in file_paths_relative
                               if x.name != 'PROTECTED']

    return file_paths_relative


def compress_list_of_files(phoenix_root: str,
                           file_list: list,
                           out_tar_ball: str) -> None:
    '''Compress list of files using tar

    In order to compress the files in the original structure from the
    PHOENIX root, the function takes relative paths from the parent of the
    PHOENIX directory. Therefore, the tar execution location is changed to the
    parental directory of the PHOENIX root, and changed back after completing
    the compression.


    Key arguments:
        phoenix_root: PHOENIX directory
        file_list: list of file paths relative to parental directory of
                   PHOENIX root, list of str.
        out_tar_ball: tar file to save
    eg)
    ```
    file_list = ['PHOENIX/GENERAL/StudyA/StudyA_metadata.csv',
                 'PHOENIX/GENERAL/StudyB/StudyB_metadata.csv']
    ```
    
    PHOENIX/
    └── GENERAL
        ├── StudyA
        │   └── StudyA_metadata.csv
        └── StudyB
            └── StudyB_metadata.csv

    Read docstring for get_updated_files.
    '''

    # convert the out_tar_ball to absolute path
    out_tar_ball_absolute = Path(out_tar_ball).absolute()

    # move to parent directory of the root
    pwd = os.getcwd()
    os.chdir(Path(phoenix_root).parent)

    file_list_str = ' '.join([str(x) for x in file_list])
    find_command = f'tar zcvf {out_tar_ball_absolute} {file_list_str}'
    proc = subprocess.Popen(find_command, shell=True, stdout=subprocess.PIPE)
    proc.wait()

    # move back to original directory
    os.chdir(pwd)


def compress_new_files(compress_db: str, phoenix_root: str,
                       out_tar_ball: str, general_only: bool = True) -> None:
    '''Find a list of new files from the last lochness to lochness sync

    Key arguments:
        compress_db: path of a csv file, which contains timestamp of each
                     lochness to lochness sync attempt (compression), str.

            eg) lochness_sync_history.csv

            timestamp
            1621470407.030176
            1621470455.6350138
        phoenix_root: PHOENIX root, str.
        out_tar_ball: compressed tarball output path, str.
        general_only: only compress the data under GENERAL if True, bool.

    Returns:
        None
    '''
    tmp_timestamp = 590403600

    if Path(compress_db).is_file():
        compress_df = pd.read_csv(compress_db, index_col=0)
        try:
            last_compress_timestamp = compress_df['timestamp'].max()
        except KeyError:
            last_compress_timestamp = tmp_timestamp
    else:
        compress_df = pd.DataFrame({'timestamp': [tmp_timestamp]})
        last_compress_timestamp = tmp_timestamp

    # get current time and update the database
    now = time()
    compress_df = pd.concat([compress_df,
                             pd.DataFrame({'timestamp': [now]})])

    # find new files and zip them
    new_file_lists = get_updated_files(phoenix_root,
                                       last_compress_timestamp,
                                       now,
                                       general_only)

    compress_list_of_files(phoenix_root, new_file_lists, out_tar_ball)

    # save database when the process completes
    compress_df.to_csv(compress_db, index=False)


def lochness_to_lochness_transfer(Lochness, general_only: bool = True):
    '''Lochness to Lochness transfer

    TODO: update dir to tmp dir
        general_only: only searches new files under GENERAL directory, bool.
    '''
    with tf.NamedTemporaryFile(suffix='tmp.tar',
                               delete=False,
                               dir='.') as tmpfilename:
        # compress
        compress_new_files(Lochness['lochness_sync_history_csv'],
                           Lochness['phoenix_root'],
                           tmpfilename.name,
                           general_only)

        # send to remote server

def send_data_over_sftp(Lochness, file_to_send: str):
    '''Send data over sftp'''

    sftp_keyring = Lochness['keyring']['lochness_to_lochness']
    host = sftp_keyring['HOST']
    username = sftp_keyring['USERNAME']
    password = sftp_keyring['PASSWORD']
    path_in_host = sftp_keyring['PATH_IN_HOST']
    port = sftp_keyring['PORT']

    transport = paramiko.Transport((host, int(port)))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    sftp.put(file_to_send, str(Path(path_in_host) / Path(file_to_send).name))
    sftp.close()
    transport.close()
