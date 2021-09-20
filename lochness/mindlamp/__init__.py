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

logger = logging.getLogger(__name__)


def get_days_to_pull(Lochness):
    ''' get module-specific delete_on_success flag with a safe default '''
    value = Lochness.get('mindlamp_days_to_pull', dict())
    if not isinstance(value, int):
        return 100
    return value


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
    '''
    logger.debug(f'exploring {subject.study}/{subject.id}')
    deidentify = deidentify_flag(Lochness, subject.study)
    logger.debug(f'deidentify for study {subject.study} is {deidentify}')

    # get keyring for mindlamp
    api_url, access_key, secret_key = mindlamp_projects(Lochness,
                                                        subject.mindlamp)

    # connect to mindlamp API sdk
    # LAMP.connect(access_key, secret_key, api_url)
    LAMP.connect(access_key, secret_key)

    # how many days of data from current time, default past 10 days
    days_to_check = get_days_to_pull(Lochness)

    # current time (ct) in UTC
    ct_utc = datetime.now(pytz.timezone('UTC'))
    ct_utc_00 = ct_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    for days_from_ct in range(days_to_check):
        # n day before current time
        time_utc_00 = ct_utc_00 - timedelta(days=days_from_ct)
        time_utc_00_ts = time.mktime(time_utc_00.timetuple()) * 1000
        time_utc_24 = time_utc_00 + timedelta(hours=24)
        time_utc_24_ts = time.mktime(time_utc_24.timetuple()) * 1000

        # date string to be used
        date_str = time_utc_00.strftime("%Y_%m_%d")

        logger.debug(f'Mindlamp {subject_id} {date_str} data pull - start')

        # Extra information for future version
        # study_id, study_name = get_study_lamp(LAMP)
        # subject_ids = get_participants_lamp(LAMP, study_id)
        subject_id = subject.mindlamp[f'mindlamp.{subject.study}'][0]

        # pull data from mindlamp
        begin = time.time()
        activity_dicts = get_activity_events_lamp(
                LAMP, subject_id,
                from_ts=time_utc_00_ts, to_ts=time_utc_24_ts)
        # activity_dicts = get_activity_events_lamp(
                # LAMP, subject_id)

        sensor_dicts = get_sensor_events_lamp(
                LAMP, subject_id,
                from_ts=time_utc_00_ts, to_ts=time_utc_24_ts)
        # sensor_dicts = get_sensor_events_lamp(
                # LAMP, subject_id)
        end = time.time()
        logger.debug(f'Mindlamp {subject_id} {date_str} data pull - complete')

        # set destination folder
        # dst_folder = tree.get('mindlamp', subject.general_folder)
        dst_folder = tree.get('mindlamp',
                              subject.protected_folder,
                              processed=False,
                              BIDS=Lochness['BIDS'])

        # store both data types
        for data_name, data_dict in zip(['activity', 'sensor'],
                                        [activity_dicts, sensor_dicts]):
            dst = os.path.join(
                    dst_folder,
                    f'{subject_id}_{subject.study}_{data_name}_'
                    f'{date_str}.json')

            jsonData = json.dumps(
                data_dict,
                sort_keys=True, indent='\t', separators=(',', ': '))

            content = jsonData.encode()

            if content.strip() == b'[]':
                logger.info(f'No mindlamp data for {subject_id} {date_str}')
                continue

            if not Path(dst).is_file():
                lochness.atomic_write(dst, content)
                logger.info(f'Mindlamp {data_name} data is saved for '
                            f'{subject_id} {date_str} (took {end-begin} s)')
            else:  # compare existing json to the new json
                crc_src = lochness.crc32(content.decode('utf-8'))
                crc_dst = lochness.crc32file(dst)
                if crc_dst != crc_src:
                    logger.warn(f'file has changed {dst}')
                    lochness.backup(dst)
                    logger.debug(f'saving {dst}')
                    lochness.atomic_write(dst, content)


def separate_out_audio_from_json(json: str):
    '''Separated json from the audio'''
    pass


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
    # if 'mindlamp' not in Keyring['lochness']:
        # raise KeyringError("lochness > mindlamp not found in keyring")

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


def get_participants_lamp(lamp: LAMP, study_id: str,
                          from_ts: str = None, to_ts: str = None) -> List[str]:
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

    Returns:
        activity_dicts: activity records, list of dict.
    '''
    activity_dicts = lamp.Activity.all_by_participant(
            subject_id, _from=from_ts, to=to_ts)['data']

    return activity_dicts


def get_sensors_lamp(lamp: LAMP, subject_id: str,
                     from_ts: str = None, to_ts: str = None) -> List[str]:

    '''Return list of sensors for a subject

    Key arguments:
        lamp: authenticated LAMP object.
        subject_id: MindLamp subject id, str.

    Returns:
        sensor_dicts: activity records, list of dict.
    '''
    sensor_dicts = lamp.Sensor.all_by_participant(
                        subject_id, _from=from_ts, to=to_ts)['data']

    return sensor_dicts


def get_activity_events_lamp(
        lamp: LAMP, subject_id: str,
        from_ts: str = None, to_ts: str = None) -> List[str]:

    '''Return list of activity events for a subject

    Key arguments:
        lamp: authenticated LAMP object.
        subject_id: MindLamp subject id, str.

    Returns:
        activity_events_dicts: activity records, list of dict.
    '''
    activity_events_dicts = lamp.ActivityEvent.all_by_participant(
                    subject_id, _from=from_ts, to=to_ts)['data']
    return activity_events_dicts


def get_sensor_events_lamp(
        lamp: LAMP, subject_id: str,
        from_ts: str = None, to_ts: str = None) -> List[str]:

    '''Return list of sensor events for a subject

    Key arguments:
        lamp: authenticated LAMP object.
        subject_id: MindLamp subject id, str.

    Returns:
        activity_dicts: activity records, list of dict.
    '''
    if not from_ts == None:
        sensor_event_dicts = lamp.SensorEvent.all_by_participant(
                        subject_id, _from=from_ts, to=to_ts)['data']
    else:
        sensor_event_dicts = lamp.SensorEvent.all_by_participant(
                        subject_id)['data']
    return sensor_event_dicts
