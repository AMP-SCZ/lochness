import os
import sys
import logging
from pathlib import Path
from string import Template
import re
from datetime import datetime


Templates = {
    'actigraphy': {
        'raw': Template('${base}/actigraphy/raw'),
        'processed': Template('${base}/actigraphy/processed')
    },
    'eeg': {
        'raw': Template('${base}/eeg/raw'),
        'processed': Template('${base}/eeg/processed')
    },
    'mri': {
        'raw': Template('${base}/mri/raw'),
        'processed': Template('${base}/mri/processed')
    },
    'mri_behav': {
        'raw': Template('${base}/mri_behav/raw'),
        'processed': Template('${base}/mri_behav/processed')
    },
    'behav_qc': {
        'raw': Template('${base}/mri_behav/processed/behav_qc'),
        'processed': Template('${base}/mri_behav/processed/behav_qc')
    },
    'mri_eye': {
        'raw': Template('${base}/mri_eye/raw/eyeTracking'),
        'processed': Template('${base}/mri_eye/processed/eyeTracking')
    },
    'phone': {
        'raw': Template('${base}/phone/raw'),
        'processed': Template('${base}/phone/processed')
    },
    'physio': {
        'raw': Template('${base}/physio/raw'),
        'processed': Template('${base}/physio/processed')
    },
    'cogassess': {
        'raw': Template('${base}/cogassess/raw'),
        'processed': Template('${base}/cogassess/processed')
    },
    'hearing': {
        'raw': Template('${base}/cogassess/raw'),
        'processed': Template('${base}/cogassess/processed'),
    },
    'retroquest': {
        'raw': Template('${base}/retroquest/raw'),
        'processed': Template('${base}/retroquest/processed')
    },
    'saliva': {
        'raw': Template('${base}/saliva/raw'),
        'processed': Template('${base}/saliva/processed')
    },
    'offsite_interview': {
        'raw': Template('${base}/offsite_interview/raw'),
        'processed': Template('${base}/offsite_interview/processed')
    },
    'onsite_interview': {
        'raw': Template('${base}/onsite_interview/raw'),
        'processed': Template('${base}/onsite_interview/processed')
    },
    'interviews': {
        'raw': Template('${base}/interviews/raw'),
        'processed': Template('${base}/interviews/processed')
    },
    'surveys': {
        'raw': Template('${base}/surveys/raw'),
        'processed': Template('${base}/surveys/processed')
    },
    'mindlamp': {
        'raw': Template('${base}/phone/raw'),
        'processed': Template('${base}/phone/processed')
    }
}

logger = logging.getLogger(__name__)

def get(data_type, base, **kwargs):
    '''get phoenix folder for a subject and data_type'''
    if data_type not in Templates:
        raise TreeError('no tree templates defined for {0}'.format(data_type))

    general_folder = Path(base).parent.parent.parent / 'GENERAL'
    protected_folder = Path(base).parent.parent.parent / 'PROTECTED'

    raw_folder = None
    processed_folder = None

    if kwargs.get('BIDS', True):   # PHOENIX to BIDS
        phoenix_id = Path(base).name  # get SUBJECT
        study = Path(base).parent.name   # get study

        if 'raw' in Templates[data_type]:
            # restructure root
            base = Path(base).parent.parent / study / 'raw'
            sub_folder = Templates[data_type]['raw'].substitute(base='')[1:]
            sub_folder = re.sub('/(raw|processed)', '', sub_folder)
            raw_folder = base / phoenix_id / sub_folder

        if 'processed' in Templates[data_type]:
            # restructure root
            base = Path(base).parent.parent / study / 'processed'
            sub_folder = Templates[data_type]['processed'].substitute(base='')[1:]

            sub_folder = re.sub('/(raw|processed)', '', sub_folder)
            processed_folder = base / phoenix_id / sub_folder

    else:
        if 'raw' in Templates[data_type]:
            raw_folder = Templates[data_type]['raw'].substitute(base=base)

        if 'processed' in Templates[data_type]:
            processed_folder = Templates[data_type]['processed'].substitute(base=base)

    if kwargs.get('makedirs', True):#  and \
            # processed_folder and not os.path.exists(processed_folder):

        protected_str = Path(re.sub('GENERAL', 'PROTECTED',
                             str(processed_folder)))
        general_str = Path(re.sub('GENERAL', 'PROTECTED',
                           str(processed_folder)))
        Path(protected_str).mkdir(exist_ok=True, parents=True)
        Path(general_str).mkdir(exist_ok=True, parents=True)
        os.chmod(protected_str, 0o00770)
        os.chmod(general_str, 0o00770)

        for path in protected_str, general_str:
            if not (path / '.log').is_file():
                with open(path / '.log', 'w') as fp:
                    fp.write(f'Created on: {datetime.today().isoformat()}')

    if kwargs.get('makedirs', True):# and \
            # raw_folder and not os.path.exists(raw_folder):
        protected_str = Path(re.sub('GENERAL', 'PROTECTED', str(raw_folder)))
        general_str = Path(re.sub('GENERAL', 'PROTECTED', str(raw_folder)))
        protected_str.mkdir(exist_ok=True, parents=True)
        general_str.mkdir(exist_ok=True, parents=True)
        os.chmod(protected_str, 0o00770)
        os.chmod(general_str, 0o00770)

        for path in protected_str, general_str:
            if not (path / '.log').is_file():
                with open(path / '.log', 'w') as fp:
                    fp.write(f'Created on: {datetime.today().isoformat()}')

    if kwargs.get('processed', True):
        return processed_folder
    else:
        return raw_folder


class TreeError(Exception):
    pass

