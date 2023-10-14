import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta
import os


def get_ok2remove_df_from_s3_log(phoenix_root: Path,
                                 days_to_keep: int = 15) -> pd.DataFrame:
    '''Load s3_log.csv file and returns okay-to-remove pd.DataFrame

    Key arguments:
        phoenix_root: Root of a PHOENIX path, Path.
        days_to_keep: number of days to keep the data, int.

    Returns:
        pd.DataFrame: each row for a path with 'timestamp', 'datatypes',
                      and 'source' as the columns.

    Note:
        Data aggregation server is limited in space and some modalities have
        a big data size, eg) mindlamp data. To save space, lochness cleans up
        files, which are transferred to s3 bucket. This function returns
        df_okay_to_remove, which contains list of file paths okay to remove,
        from the s3_log.
    '''
    s3_log_file = phoenix_root / 's3_log.csv'

    if Path(s3_log_file).is_file():
        s3_log_df = pd.read_csv(s3_log_file, index_col=0)
        s3_log_df['timestamp'] = pd.to_datetime(s3_log_df['timestamp'])
    else:
        s3_log_df = pd.DataFrame()

    date_point = datetime.now() - timedelta(days=days_to_keep)
    df_okay_to_remove = s3_log_df[s3_log_df['timestamp'] < date_point]

    # There are files re-sent to the S3, which should not be deleted
    df_not_okay_to_remove = s3_log_df[s3_log_df['timestamp'] >= date_point]
    df_okay_to_remove = df_okay_to_remove[
            ~df_okay_to_remove.source.isin(df_not_okay_to_remove.source)]

    # XNAT
    # since lochness.xnat pulls a zip file to a temporary file and extracts
    # to a directory, a root directory of the saved MRI data needs to be added
    mri_df = df_okay_to_remove[df_okay_to_remove['datatypes'] == 'mri']
    mri_root_names = mri_df['source'].str.extract(r'([\w/]*/mri/\w+)/')
    mri_root_names.drop_duplicates(inplace=True)
    mri_root_names.columns = ['source']
    mri_root_names.dropna(inplace=True)
    df_okay_to_remove = pd.concat([df_okay_to_remove, mri_root_names])

    return df_okay_to_remove


def is_path_ok2remove(phoenix_root: Path,
                      file_path: Path,
                      df_okay_to_remove: pd.DataFrame) -> bool:
    '''Return True if a path is okay to remove

    Key Arguments:
        phoenix_root: path of the PHOENIX directory, Path.
        file_path: path of a file to check if okay to remove, Path.
        df_okay_to_remove: database for files okay to remove, pd.Dataframe.

    Returns:
        Boolean of okay to remove, bool.
    '''
    # file patterns not to delete
    patterns_not_to_delete = [r'metadata.csv', 's3_log.csv']
    for pattern in patterns_not_to_delete:
        if re.search(pattern, str(file_path)):
            return False

    # get path of the file relative to the root of PHOENIX directory
    try:
        file_path_relative = str(file_path.relative_to(phoenix_root.parent))
    except ValueError:
        return False

    if file_path_relative in df_okay_to_remove['source'].tolist():
        return True
    else:
        return False


def make_deleted_structure(phoenix_root: Path,
                           file_path: Path,
                           removed_phoenix_root: Path) -> None:
    '''Create PHOENIX structure of deleted files to keep track of deleted files

    Key Arguments:
        phoenix_root: Root of a PHOENIX path, Path.
        file_path: path of the file deleted, Path.
        removed_phoenix_root: root of the new PHOENIX directory to keep track
                              of the file being deleted, Path.
    '''
    file_path_relative = file_path.relative_to(phoenix_root.parent)

    file_path_new = Path(removed_phoenix_root) / \
        file_path_relative.relative_to('PHOENIX')
    file_path_new.parent.mkdir(parents=True, exist_ok=True)
    file_path_new.touch()


def rm_transferred_files_under_phoenix(
        phoenix_root: Path,
        days_to_keep: int = 30,
        removed_df_loc: Path = None,
        removed_phoenix_root: Path = None) -> None:
    '''Remove s3 transferred files from PHOENIX

    Key Arguments:
        phoenix_root: path of the PHOENIX directory, Path
        days_to_keep: days to keep the data for, str. default = 15.
        removed_df_loc: path of a csv file to keep track of the removed files,
                        Path.
        removed_phoenix_root: new phoenix directory to keep the track of
                              removed files, Path. If None, the function does
                              not create this new phoenix directory.
    Returns:
        None
    '''
    phoenix_root = Path(phoenix_root)
    df_okay_to_remove = get_ok2remove_df_from_s3_log(Path(phoenix_root),
                                                     days_to_keep)

    if removed_df_loc is None:
        removed_df_loc = phoenix_root / 'removed_files.csv'

    if Path(removed_df_loc).is_file():
        df_removed = pd.read_csv(removed_df_loc, index_col=0)
    else:
        df_removed = pd.DataFrame()

    for root, _, files in os.walk(phoenix_root):
        for file in files:
            file_path = Path(root) / file
            if is_path_ok2remove(phoenix_root, file_path, df_okay_to_remove):
            
                # os.remove(file_path)  # safelock

                df_removed_tmp = pd.DataFrame({
                    'source': [file_path],
                    'removal_date': datetime.now()})
                df_removed = pd.concat([df_removed, df_removed_tmp])
                if removed_phoenix_root is not None:
                    make_deleted_structure(Path(phoenix_root),
                                           file_path,
                                           Path(removed_phoenix_root))

    df_removed.to_csv(removed_df_loc)


def is_transferred_and_removed(Lochness,
                               destination: str,
                               removed_df_loc: str = None) -> bool:
    '''Check if a file path is a path that Lochness has removed previously

    Key Arguments:
        Lochness: Lochness object, requires 'phoenix_root', dict.
        destination: path of a file to save a data by a lochness submodule,
                     str.
        removed_df_loc: path of a 'removed_files.csv', str.

    Returns:
        bool: True if removed previously
    '''
    if removed_df_loc is None:
        try:
            removed_df_loc = Path(Lochness['removed_df_loc'])
        except KeyError:
            removed_df_loc = 'not_defined'



    if Path(removed_df_loc).is_file():
        df_removed = pd.read_csv(removed_df_loc, index_col=0)
    else:
        df_removed = pd.DataFrame()
        df_removed['source'] = []

    if str(destination) in df_removed.source.tolist():
        return True
    else:
        return False
