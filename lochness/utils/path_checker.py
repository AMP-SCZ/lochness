"""
Checks Mediaflux file paths for deviations from the standard operating
procedures (SOP).

Note: This module checks files at source (Mediaflux) and not at the
destination (local storage). So this will check files that will be
pulled by Lochness and those that will not be pulled by Lochness.
"""
import pandas as pd
import re
from pathlib import Path
from typing import List


def ampscz_id_validate(some_id: str):
    # https://github.com/AMP-SCZ/subject-id-validator
    # Basic checks: len == 7, first two chars are not numbers,
    # all other chars are numbers
    if type(some_id) != str:
        return False

    if len(some_id) != 7:
        return False

    if not (some_id[0].isalpha() and some_id[1].isalpha()):
        return False

    if not all([n.isdecimal() for n in some_id[2:6]]):
        return False

    # Convert ID to array of numbers, excluding check digit
    id_array = []
    id_array.append(ord(some_id[0].upper()))
    id_array.append(ord(some_id[1].upper()))
    id_array = id_array + list(some_id[2:6])

    # Use check digit algorithm to generate correct check digit
    check_digit_array = []

    for pos in range(len(id_array)):
        check_digit_array.append(int(id_array[pos]) * (pos+1))
    check_digit = sum(check_digit_array) % 10

    # Check correct check digit against entered ID
    if int(some_id[6]) != check_digit:
        return False

    return True


def ampscz_penn_validate(some_id: str):
    '''Validate PENN ID field, which is in AB01234_n pattern'''
    if ampscz_id_validate(some_id):
        return True

    if len(some_id) != 9:
        return False

    # check if ampscz id is followed by '_'
    if not some_id[7] in ['_']:
        return False

    # check if last str is digit
    if not some_id[-1] in '123456789':
        return False

    # now check on the ID part of the scring
    ampscz_id = some_id[:7]
    return ampscz_id_validate(ampscz_id)


def nth_item_from_path(df: pd.DataFrame, n: int) -> pd.Series:
    '''Return n th item from pd.Series of paths'''
    return df['file_path'].str.split('/').str[n]


def update_eeg_check(df: pd.DataFrame) -> None:
    '''Check logics in rows for EEG modality'''
    eeg_index = df[df['modality'] == 'EEG'].index
    eeg_df = df.loc[eeg_index]
    eeg_df['file_check'] = eeg_df['file_name'].str.match(
            '[A-Z]{2}\d{5}_eeg_\d{4}\d{2}\d{2}.*.zip$')
    df.loc[eeg_index] = eeg_df


def update_mri_check(df: pd.DataFrame) -> pd.DataFrame:
    '''Check logics in rows for MRI modality'''
    mri_index = df[df['modality'] == 'MRI'].index
    mri_df = df.loc[mri_index]
    mri_df['file_check'] = mri_df['file_name'].str.match(
            '[A-Z]{2}\d{5}_MR_\d{4}_\d{2}_\d{2}_\d.[Zz][Ii][Pp]')

    df.loc[mri_index] = mri_df


def update_actigraphy_check(df: pd.DataFrame) -> pd.DataFrame:
    '''Check logics in rows for Actigraphy modality'''
    act_index = df[df['modality'] == 'Actigraphy'].index
    act_df = df.loc[act_index]
    act_df['file_check'] = act_df['file_name'].str.match(
            '[A-Z]{2}\d{5}_\d{5}_\d{4}\d{2}\d{2}.cwa')


    df.loc[act_index] = act_df


def update_interviews_check(df: pd.DataFrame) -> pd.DataFrame:
    '''Check logics in rows for Interviews modality'''
    int_index = df[df['modality'] == 'Interviews'].index

    # subject location is different
    int_df = df.loc[int_index]
    int_df['subject'] = nth_item_from_path(int_df, 3)
    int_df['subject_check'] = int_df['subject'].apply(ampscz_id_validate)
    df.loc[int_index] = int_df


def update_interviews_transcript_check(df: pd.DataFrame) -> pd.DataFrame:
    '''Check logics in rows for Interviews transcripts'''
    transcript_int_index = df[
            (df.modality=='Interviews') &
            (df.subject.isin(['For_review', 'Approved']))].index

    transcript_int_df = df.loc[transcript_int_index]
    transcript_int_df['subject'] = nth_item_from_path(transcript_int_df, 4)
    transcript_int_df['subject_check'] = transcript_int_df['subject'
            ].apply(ampscz_id_validate)

    # make sure there is no duplicated subject directory
    fourth_item_in_path = nth_item_from_path(transcript_int_df, 4)  # dirname
    fifth_item_in_path = nth_item_from_path(transcript_int_df, 5)  # file name
    transcript_int_df['modality_check'] = \
            fourth_item_in_path != fifth_item_in_path

    # check site and AMPSCZ IDs in the transcript file name
    for index, row in transcript_int_df.iterrows():
        if not row['subject_check']:
            try:
                #TODO: fix this
                transcript_int_df.loc[index, 'subject'] = re.search(
                    r'[A-Z]{2}\d{5}', row['subject']).group(0)
                row['subject'] = transcript_int_df.loc[index, 'subject']
                row['subject_check'] = ampscz_id_validate(row['subject'])
                transcript_int_df.loc[index,
                                      'subject_check'] = row['subject_check']
            except:
                pass
        subject = row['subject']
        site = row['site']
        transcript_int_df.loc[index, 'file_check'] = re.match(
                f'(Prescient|){site}_{subject}_'
                'interviewAudioTranscript_(open|psychs)_day[-\d]+_session\d+.txt',
                row['file_name'])

    df.loc[transcript_int_index] = transcript_int_df


def update_interviews_video_check(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Check logics in rows for Interviews video

    Check the following:
        - Zoom name in the parent directory
        - Zoom name should be in the format of 'YYYY-MM-DD HH.MM.SS <name>'
        - Video files should be under the zoom folder

    Args:
        df (pd.DataFrame): The DataFrame containing the data.

    Returns:
        pd.DataFrame: The updated DataFrame.
    '''
    # interviews transcript
    video_int_index = df[
        (df.modality == "Interviews")
        & (
            (df.file_name.str.endswith(".mp4"))
            | (df.file_name.str.endswith(".conf"))
        )
    ].index

    video_int_df = df.loc[video_int_index]

    video_int_df["zoom_name"] = video_int_df["parent_dir"].str.extract(
        "\d{4}-\d{2}-\d{2} \d{2}\.\d{2}\.\d{2} (.+)"
    )

    # video files must be under the zoom folder
    video_int_df["file_check"] = ~video_int_df["zoom_name"].isnull()

    df.loc[video_int_index] = video_int_df


def update_interviews_teams_data_check(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Check logics in rows for Interviews with WAV audio files.

    Validate the following:
        - The directory structure
        - The file name pattern
            - The file name should be in the format of 'YYYYMMDDHHMMSS.wav'

    Args:
        df (pd.DataFrame): The DataFrame containing the data.

    Returns:
        pd.DataFrame: The updated DataFrame.
    '''
    # interviews transcript
    video_int_index = df[
            (df.modality=='Interviews') &
            (df.file_name.str.lower().str.endswith('.wav'))
            ].index

    video_int_df = df.loc[video_int_index]

    # directory check
    video_int_df['directory_check'] = video_int_df['parent_dir'] \
            == video_int_df['subject']

    # file name pattern check
    video_int_df['file_pattern_check'] = video_int_df['file_name'].str.match(
        r'\d{4}\d{2}\d{2}\d{6}(wav|WAV)'
    )
    video_int_df['file_check'] = video_int_df['directory_check'] & \
        video_int_df['file_name']
    df.loc[video_int_index] = video_int_df


def update_interviews_audio_check(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Check logics in rows for Interviews audio

    Skip the following files:
        - 'playback.m3u' files
        - 'Audio Record' directory
        - 'Zoomver.tag' files
        - files ending with '.log'

    Skip here means to set the 'file_check' column to True, essentially
    ignoring these files.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.

    Returns:
        pd.DataFrame: The updated DataFrame.
    '''
    # interviews audio
    audio_int_index = df[
        (df.modality == 'Interviews') & (df.file_name.str.endswith('.m4a'))
    ].index

    audio_int_df = df.loc[audio_int_index]
    audio_int_df['zoom_name'] = audio_int_df['parent_dir'].str.extract(
        '\d{4}-\d{2}-\d{2} \d{2}\.\d{2}\.\d{2} (.+)'
    )

    # audio files need to be under either the zoom or Audio Record folder
    audio_int_df['file_check_1'] = ~audio_int_df['zoom_name'].isnull()
    audio_int_df['file_check_2'] = audio_int_df['parent_dir'] == 'Audio Record'
    audio_int_df['file_check'] = audio_int_df[
        ['file_check_1', 'file_check_2']].any(axis=1)
    audio_int_df.drop(['file_check_1', 'file_check_2'], axis=1, inplace=True)

    df.loc[audio_int_index] = audio_int_df

    # ignore playback.m3u audio files
    audio_int_index = df[
        (df.modality == 'Interviews') & (df.file_name == 'playback.m3u')
    ].index
    audio_int_df = df.loc[audio_int_index]
    audio_int_df['file_check'] = True
    df.loc[audio_int_index] = audio_int_df

    # ignore 'Audio Record' folder
    rec_index = df[
        (df.modality == 'Interviews') & (df.file_name.str.endswith('Audio Record'))
    ].index
    rec_df = df.loc[rec_index]
    rec_df['file_check'] = True
    df.loc[rec_index] = rec_df

    # ignore 'Zoomver.tag" files
    zoomver_index = df[
        (df.modality == 'Interviews') & (df.file_name.str.endswith('ver.tag'))
    ].index
    zoomver_df = df.loc[zoomver_index]
    zoomver_df['file_check'] = True
    df.loc[zoomver_index] = zoomver_df

    # Ignore files ending with '.log'
    log_index = df[
        (df.modality == 'Interviews') & (df.file_name.str.endswith('.log'))
    ].index
    log_df = df.loc[log_index]
    log_df['file_check'] = True
    df.loc[log_index] = log_df


def update_by_adding_notes(df: pd.DataFrame) -> None:
    '''Add notes to the table'''
    pass


def update_by_removing_unused_files(df: pd.DataFrame) -> None:
    '''
    Remove unused files.

    The following files will be removed:
        - .DS_Store
        - *.conf
        - chat.txt
        - Files under TRANSCRIPTS/For review

    Args:
        df (pd.DataFrame): The DataFrame containing the data.

    Returns:
        None
    '''
    # .DS_Store
    ds_store_index = df[df.file_name == '.DS_Store'].index
    df.drop(ds_store_index, inplace=True)

    # *.conf files
    recording_conf_index = df[df.file_name.str.endswith('.conf')].index
    df.drop(recording_conf_index, inplace=True)

    # chat.txt files
    chat_txt_index = df[df.file_name == 'chat.txt'].index
    df.drop(chat_txt_index, inplace=True)

    # Ignore For review
    for_review_index = df[df.file_path.str.contains(
        'TRANSCRIPTS/For review')].index
    df.drop(for_review_index, inplace=True)

def update_skipped_av_files(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the 'file_check' column in the DataFrame based on the 'parent_dir' column.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.

    Returns:
        pd.DataFrame: The updated DataFrame.
    """
    for idx, row in df.iterrows():
        if "Extraneous files" in row["file_path"]:
            df.loc[idx, "file_check"] = True
        elif "Additional interview files" in row["file_path"]:
            df.loc[idx, "file_check"] = True
    return df


def update_by_removing_genetics_and_fluids(df: pd.DataFrame) -> None:
    '''Remove files under GeneticsAndFluids directory'''
    # .DS_Store
    genetics_index = df[df['modality']=='GeneticsAndFluids'].index
    df.drop(genetics_index, inplace=True)


def update_by_checking_against_subject_list(
        df: pd.DataFrame,
        subject_id_list: List[str]) -> None:
    '''pass'''
    df['exist_in_db'] = df['subject'].isin(subject_id_list).fillna(False)
    missing_in_db_index = df[~df.exist_in_db].index.tolist()

    df.loc[missing_in_db_index, 'notes'] = 'Subject missing from database'


def check_file_path_df(df: pd.DataFrame,
                       subject_id_list: list) -> pd.DataFrame:
    '''Check file_path column for deviations from SOP'''

    df['site'] = nth_item_from_path(df, 0)

    df['modality_dir'] = nth_item_from_path(df, 1)
    df['modality'] = df['modality_dir'].str.split('_').str[1]

    # subject
    df['subject'] = nth_item_from_path(df, 2)
    df['file_name'] = nth_item_from_path(df, -1)
    df['parent_dir'] = nth_item_from_path(df, -2)

    # main check logics start here
    df['site_check'] = df.site.str.match('Prescient|Pronet')

    #  validate AMPSCZ ID
    df['subject_check'] = df['subject'].apply(ampscz_id_validate)

    df['modality_check'] = df['modality'].str.contains(
            'MRI|EEG|Actigraphy|Interviews').fillna(False)

    # file name pattern checks
    df['file_check'] = False

    # below are functions for files directly under the subject directory in
    # the source
    update_eeg_check(df)
    update_actigraphy_check(df)
    update_mri_check(df)

    update_interviews_check(df)
    update_interviews_transcript_check(df)
    update_interviews_teams_data_check(df)
    update_interviews_video_check(df)
    update_interviews_audio_check(df)
    update_skipped_av_files(df)

    # ignore genetics and fluids
    update_by_removing_genetics_and_fluids(df)

    # ignore .DS_Store, *.conf, chat.txt
    update_by_removing_unused_files(df)

    # check if the subject exist in metadata
    update_by_checking_against_subject_list(df, subject_id_list)

    # check if the letters in front of the subject matches the site
    site_mismatch = df[df['subject'].str[:2] != df['site'].str[-2:]].index
    df.loc[site_mismatch, 'subject_check'] = False

    # add notes
    update_by_adding_notes(df)

    # fill na as False for the check columns
    for i in ['modality', 'subject', 'file']:
        df[f"{i}_check"] = df[f"{i}_check"].fillna(False)


    df['final_check'] = df[
            ['modality_check', 'file_check']
        ].all(axis=1)

    return df


def print_deviation(df):
    '''Print deviations from the df'''
    prev_row = 'x'
    num = 0

    lines = []
    for index, row in df[(~df.final_check) &
                         (~df.file_path.str.endswith('dcm'))
                     ].sort_values('file_path').iterrows():

        new_string = re.sub(prev_row, ' '*len(prev_row), row['file_path'])
        print(new_string)
        lines.append(new_string)
        prev_row = str(Path(row['file_path']).parent) + '/'

    return lines


