import LAMP
import logging
import lochness
import os
import lochness.net as net
import sys
import json
import lochness.tree as tree
from io import BytesIO
from pathlib import Path
from typing import Tuple, List
import pytz
import time
from datetime import datetime, timedelta
import base64
import re
import tempfile as tf
from lochness.cleaner import is_transferred_and_removed
from lochness.utils.checksum import get_sha

logger = logging.getLogger(__name__)

LIMIT = 1000000


def get_days_to_pull(Lochness):
    '''Reads mindlamp_days_to_pull from Lochness loaded from the config.yml

    If this variable is missing in the config.yml, set it to pull previous
    100 days of data from today, from the mindlamp server. '''

    value = Lochness.get('mindlamp_days_to_pull', 100)

    return value


def get_audio_out_from_content(activity_dicts, audio_file_name):
    '''Separate out audio data from the content pulled from mindlamp API

    Key Arguments:
        activity_dicts: list of activity dictionaries, list.
        audio_file_name: name of the audio file to be saved, str.

    Returns:
        activity_dicts_wo_sound: list of activity dictionaries without the
                                 sound data. (sound data replaced to
                                 'SOUND_{num}')

    Notes:
        `num` variables used in the function to capture all audio data, when
        there is more than one recording each day.
    '''
    activity_dicts_wo_sound = []

    num = 0
    for activity_events_dicts in activity_dicts:
        if 'url' in activity_events_dicts['static_data']:
            audio = activity_events_dicts['static_data']['url']
            activity_events_dicts['static_data']['url'] = f'SOUND_{num}'

            decode_bytes = base64.b64decode(audio.split(',')[1])

            # just in case there are more than one recording per day
            with open(re.sub(r'.mp3', f'_{num}.mp3', audio_file_name),
                      'wb') as f:
                f.write(decode_bytes)
            num += 1

        activity_dicts_wo_sound.append(activity_events_dicts)

    return activity_dicts_wo_sound


@net.retry(max_attempts=5)
def sync(Lochness: 'lochness.config',
         subject: 'subject.metadata',
         dry: bool = False):
    '''Sync mindlamp data

    To do:
    - Currently the mindlamp participant id is set by mindlamp, when the
      participant object is created. API can download all list of participant
      ids, but there is no mapping of which id corresponds to which subject.
    - Above information has to be added to the metadata.csv file.
    - Add ApiExceptions
    - This function is downloading X days of previous data, regardless of
      existence of the destination data
    '''
    logger.debug(f'exploring {subject.study}/{subject.id}')
    deidentify = deidentify_flag(Lochness, subject.study)
    logger.debug(f'deidentify for study {subject.study} is {deidentify}')

    # get keyring for mindlamp
    if len(subject.mindlamp.keys()) == 0:
        return

    api_url, access_key, secret_key = mindlamp_projects(Lochness,
                                                        subject.mindlamp)

    # connect to mindlamp API sdk
    LAMP.connect(access_key, secret_key, api_url)

    # how many days of data from current time, default past 100 days
    days_to_check = get_days_to_pull(Lochness)

    # current time (ct) in UTC
    ct_utc = datetime.now(pytz.timezone('UTC'))

    # set the cut off point as UTC 00:00:00.00
    ct_utc_00 = ct_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    # Extra information for future version
    # study_id, study_name = get_study_lamp(LAMP)
    # subject_ids = get_participants_lamp(LAMP, study_id)
    subject_id = subject.mindlamp[f'mindlamp.{subject.study}'][0]

    # extract consent date to only download data after consent
    consent_date = datetime.strptime(subject.consent, '%Y-%m-%d').replace(
            tzinfo=pytz.timezone('UTC'))

    # set destination folder
    dst_folder = tree.get('mindlamp',
                          subject.protected_folder,
                          processed=False,
                          BIDS=Lochness['BIDS'],
                          makedirs=True)

    # the loop below downloads all data from mindlamp from the current date
    # to (current date - 100 days), overwriting pre-downloaded files.
    for days_from_ct in reversed(range(days_to_check)):
        # get time range: n days before current date
        # 1000 has been multiplied to match timestamp format
        time_utc_00 = ct_utc_00 - timedelta(days=days_from_ct)
        time_utc_00_ts = time.mktime(time_utc_00.timetuple()) * 1000
        time_utc_24 = time_utc_00 + timedelta(hours=24)
        time_utc_24_ts = time.mktime(time_utc_24.timetuple()) * 1000

        # if the before consent_date, do not download the data
        if consent_date > time_utc_00:
            continue

        # date string to be used in the file name
        date_str = time_utc_00.strftime("%Y_%m_%d")

        logger.debug(f'Mindlamp {subject_id} {date_str} data pull - start')

        # store both data types
        for data_name in ['activity', 'sensor']:
            dst = Path(dst_folder) / \
                f'{subject_id}_{subject.study}_{data_name}_{date_str}.json'

            function_to_execute = get_activity_events_lamp \
                if data_name == 'activity' else get_sensor_events_lamp

            # do not re-download already transferred & removed data
            # if is_transferred_and_removed(Lochness, dst):
            #    continue

            prev_file_sha256 = ''  # set previous sha as empty
            checksum_file = dst.parent / f'.check_sum_{dst.name}'
            if days_from_ct >= 2 and Path(dst).is_file():
                # if the days_from_ct is more than two days, the mindlmap data
                # on the mindlamp server should not change, thus no need to
                # re-download data for checking checksum.
                if checksum_file.is_file():
                    logger.debug(f'{data_name} data has been downloaded for '
                                 f'{date_str} - skip downloading')
                    continue

            elif days_from_ct < 2 and Path(dst).is_file():
                # potentially within 24 hours from the data acquisition, data
                # may change so check if there is any changes in the data on
                # the source
                if checksum_file.is_file():
                    logger.debug(f'{data_name} data has been downloaded more '
                                 'than once for checksum')

                    # read checksum of the existing file
                    with open(checksum_file, 'r') as fp:
                        prev_file_sha256 = fp.read().strip()

            # pull data from mindlamp
            begin = time.time()
            try:
                data_dict = function_to_execute(LAMP, subject_id,
                                                from_ts=time_utc_00_ts,
                                                to_ts=time_utc_24_ts)
            except Exception as e:
                print(e)
                continue

            end = time.time()
            logger.debug(
                f'Mindlamp {subject_id} {date_str} {data_name} data pull'
                f' - completed in ({end - begin} seconds)')

            # separate out audio data from the activity dictionary
            if data_name == 'activity' and data_dict:
                sound_dst = os.path.join(
                        dst_folder,
                        f'{subject_id}_{subject.study}_{data_name}_'
                        f'{date_str}_sound.mp3')
                # if is_transferred_and_removed(Lochness, sound_dst):
                    # continue
                data_dict = get_audio_out_from_content(data_dict, sound_dst)

            jsonData = json.dumps(data_dict, sort_keys=True,
                                  indent=3, separators=(',', ': '))
            content = jsonData.encode()
            if content.strip() == b'[]':
                logger.info(f'No mindlamp data for {subject_id} {date_str}')
                continue

            with tf.NamedTemporaryFile(suffix='tmp.json') as tmpfilename:
                lochness.atomic_write(tmpfilename.name, content)
                new_file_sha256 = get_sha(tmpfilename.name)
                # if checksum is the same don't overwrite the existing data
                if new_file_sha256 == prev_file_sha256:
                    continue
                else:
                    with open(checksum_file, 'w') as fp:
                        fp.write(new_file_sha256)

            lochness.atomic_write(dst, content)
            logger.info(f'Mindlamp {data_name} data is saved for '
                        f'{subject_id} {date_str} (took {end-begin} s)')


def deidentify_flag(Lochness, study):
    ''' get study specific deidentify flag with a safe default '''
    value = Lochness.get('mindlamp', dict()) \
                    .get(study, dict()) \
                    .get('deidentify', False)
    # if this is anything but a boolean, just return False
    if not isinstance(value, bool):
        return False
    return value


def mindlamp_projects(Lochness: 'lochness.config',
                      mindlamp_instance: 'subject.mindlamp.item'):
    '''get mindlamp api_url and api_key for a phoenix study'''
    Keyring = Lochness['keyring']
    key_name = list(mindlamp_instance.keys())[0]  # mindlamp.StudyA

    # Assertations
    # check for mandatory keyring items

    if key_name not in Keyring:
        raise KeyringError(f"{key_name} not found in keyring")

    if 'URL' not in Keyring[key_name]:
        raise KeyringError(f"{key_name} > URL not found in keyring")

    if 'ACCESS_KEY' not in Keyring[key_name]:
        raise KeyringError(f"{key_name} > ACCESS_KEY "
                            "not found in keyring")

    if 'SECRET_KEY' not in Keyring[key_name]:
        raise KeyringError(f"{key_name} > SECRET_KEY "
                            "not found in keyring")

    api_url = Keyring[key_name]['URL'].rstrip('/')
    access_key = Keyring[key_name]['ACCESS_KEY']
    secret_key = Keyring[key_name]['SECRET_KEY']

    return api_url, access_key, secret_key


class KeyringError(Exception):
    pass


def get_study_lamp(lamp: LAMP) -> Tuple[str, str]:
    '''Return study id and name

    Assert there is only single study under the authenticated MindLamp.

    Key arguments:
        lamp: authenticated LAMP object.

    Returns:
        (study_id, study_name): study id and study objects, Tuple.
    '''
    study_objs = lamp.Study.all_by_researcher('me')['data']
    # assert len(study_objs) == 1, "There are more than one MindLamp study"
    study_obj = study_objs[0]
    return study_obj['id'], study_obj['name']


def get_participants_lamp(lamp: LAMP, study_id: str) -> List[str]:
    '''Return subject ids for a study

    Key arguments:
        lamp: authenticated LAMP object.
        study_id: MindLamp study id, str.

    Returns:
        subject_ids: participant ids, list of str.
    '''
    subject_objs = lamp.Participant.all_by_study(study_id)['data']
    subject_ids = [x['id'] for x in subject_objs]

    return subject_ids


def get_activities_lamp(lamp: LAMP, subject_id: str,
                        from_ts: str = None, to_ts: str = None) -> List[str]:
    '''Return list of activities for a subject

    Key arguments:
        lamp: authenticated LAMP object.
        subject_id: MindLamp subject id, str.
        from_ts: 13 digit timestamp used to limit the api call from, str.
        to_ts: 13 digit timestamp used to limit the api call to, str.

    Returns:
        activity_dicts: activity records, list of dict.
    '''
    activity_dicts = lamp.Activity.all_by_participant(
            subject_id, _from=from_ts, to=to_ts, _limit=LIMIT)['data']

    return activity_dicts


def get_sensors_lamp(lamp: LAMP, subject_id: str,
                     from_ts: str = None, to_ts: str = None) -> List[str]:

    '''Return list of sensors for a subject

    Key arguments:
        lamp: authenticated LAMP object.
        subject_id: MindLamp subject id, str.
        from_ts: 13 digit timestamp used to limit the api call from, str.
        to_ts: 13 digit timestamp used to limit the api call to, str.

    Returns:
        sensor_dicts: activity records, list of dict.
    '''
    sensor_dicts = lamp.Sensor.all_by_participant(
                        subject_id, _from=from_ts, to=to_ts,
                        _limit=1000000)['data']

    return sensor_dicts


def get_activity_events_lamp(
        lamp: LAMP, subject_id: str,
        from_ts: str = None, to_ts: str = None) -> List[str]:

    '''Return list of activity events for a subject

    Key arguments:
        lamp: authenticated LAMP object.
        subject_id: MindLamp subject id, str.
        from_ts: 13 digit timestamp used to limit the api call from, str.
        to_ts: 13 digit timestamp used to limit the api call to, str.

    Returns:
        activity_events_dicts: activity records, list of dict.
    '''
    activity_events_dicts = lamp.ActivityEvent.all_by_participant(
                    subject_id, _from=from_ts, to=to_ts,
                    _limit=LIMIT)['data']
    return activity_events_dicts


def get_sensor_events_lamp(
        lamp: LAMP, subject_id: str,
        from_ts: str = None, to_ts: str = None) -> List[str]:

    '''Return list of sensor events for a subject

    Key arguments:
        lamp: authenticated LAMP object.
        subject_id: MindLamp subject id, str.
        from_ts: 13 digit timestamp used to limit the api call from, str.
        to_ts: 13 digit timestamp used to limit the api call to, str.

    Returns:
        activity_dicts: activity records, list of dict.
    '''
    timestamp_now = lambda: int(round(time.time() * 1000))
    timestamp_limit = (1 * 24 * 60 * 60 * 1000) # 1 day of data

    timestamp_limit = to_ts - from_ts

    sensor_event_dicts = []
    if from_ts is not None:
        while True:
            res = lamp.SensorEvent.all_by_participant(
                            subject_id, _from=from_ts, to=to_ts,
                            _limit=LIMIT)
            if 'error' in res:
                raise RuntimeError(res["error"])

            chunk = res['data'] if 'data' in res else []
            sensor_event_dicts += chunk

            if not chunk or int(to_ts) - int(chunk[-1]['timestamp']) == 0:
                break

            to_ts = chunk[-1]['timestamp']
    else:
        sensor_event_dicts = lamp.SensorEvent.all_by_participant(
                        subject_id, _limit=LIMIT)['data']

    return sensor_event_dicts
