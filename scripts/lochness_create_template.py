#!/usr/bin/env python
'''
Create template lochness directory
'''
import lochness
from lochness.keyring import pretty_print_dict
from pathlib import Path
import argparse as ap
import sys
import re
from typing import List
import pandas as pd
import json
import importlib
import getpass
from phoenix_generator import main as pg


lochness_root = Path(lochness.__file__).parent.parent
lochness_test_dir = lochness_root / 'test'


class ArgsForPheonix(object):
    def __init__(self, study, dir):
        self.study = study
        self.dir = dir
        self.verbose = False


def create_lochness_template(args):
    '''Create template for lochness'''
    # make sources small
    args.sources = [x.lower() for x in args.sources]
    args.outdir = Path(args.outdir).absolute()

    # make lochness root directory
    Path(args.outdir).mkdir(exist_ok=True)

    # PHOENIX root
    phoenix_root = Path(args.outdir) / 'PHOENIX'

    # create PHOENIX directory
    for study in args.studies:
        argsForPheonix = ArgsForPheonix(study, phoenix_root)
        try:
            pg(argsForPheonix)
        except SystemExit:
            pass
        metadata = phoenix_root / 'GENERAL' / study / f'{study}_metadata.csv'

        # create example metadata
        create_example_meta_file_advanced(metadata, study, args.sources)

    # create det_csv
    if not Path(args.det_csv).is_file():
        args.det_csv = args.outdir / 'data_entry_trigger_database.csv'


    # create pii table
    if not Path(args.pii_csv).is_file():
        args.pii_csv = args.outdir / 'pii_convert.csv'
        df = pd.DataFrame({
            'pii_label_string': [
                'address', 'phone_number', 'date',
                'patient_name', 'subject_name'],
            'process': [
                'remove', 'random_number', 'change_date',
                'random_string', 'replace_with_subject_id']
            })
        df.to_csv(args.pii_csv)

    # link lochness_sycn_history timestamp db
    if not Path(args.lochness_sync_history_csv).is_file():
        args.lochness_sync_history_csv = args.outdir / \
                'lochness_sync_history.csv'

    # create config
    config_loc = args.outdir / 'config.yml'
    create_config_template(config_loc, args)

    # create keyring
    keyring_loc = args.outdir / 'lochness.json'
    encrypt_keyring_loc = args.outdir / '.lochness.enc'
    create_keyring_template(keyring_loc, args)

    # write commands for the user to run after editing config and keyring
    write_commands_needed(args, config_loc, keyring_loc, encrypt_keyring_loc)


def write_commands_needed(args: 'argparse',
                          config_loc: Path,
                          keyring_loc: Path,
                          encrypt_keyring_loc: Path) -> None:
    '''Write commands'''
    # encrypt_command.sh
    with open(args.outdir / '1_encrypt_command.sh', 'w') as f:
        f.write('#!/bin/bash\n')
        f.write('# run this command to encrypt the edited keyring '
                '(lochness.json)\n')
        f.write('# eg) bash 1_encrypt_command.sh\n')
        command = f'crypt.py --encrypt {keyring_loc} ' \
                  f'-o {encrypt_keyring_loc}\n'
        f.write(command)

    # sync_command.sh
    with open(args.outdir / '2_sync_command.sh', 'w') as f:
        f.write('#!/bin/bash\n')
        f.write('# run this command to run sync.py\n')
        f.write('# eg) bash 2_sync_command.sh\n')

        if args.lochness_sync_send:
            if args.s3:
                command = f"sync.py -c {config_loc} \
                       --studies {' '.join(args.studies)} \
                       --source {' '.join(args.sources)} \
                       --lochness_sync_send --s3 \
                       --debug --continuous \
                       --log-file {args.outdir}/log.txt \
                       --daily_summary \
                       --remove_old_files\n"
            elif args.rsync:
                command = f"sync.py -c {config_loc} \
                        --studies {' '.join(args.studies)} \
                        --source {' '.join(args.sources)} \
                        --lochness_sync_send --rsync \
                        --log-file {args.outdir}/log.txt \
                        --debug --continuous \
                        --remove_old_files\n"
            else:
                command = f"sync.py -c {config_loc} \
                        --studies {' '.join(args.studies)} \
                        --source {' '.join(args.sources)} \
                        --lochness_sync_send --s3 \
                        --log-file {args.outdir}/log.txt \
                        --debug --continuous \
                        --remove_old_files\n"
        
        command = re.sub('\s\s+', ' \\\n\t', command)
        f.write(command)


def create_keyring_template(keyring_loc: Path, args: object) -> None:
    '''Create keyring template'''

    template_dict = {}
    template_dict['lochness'] = {}

    if 'redcap' in args.sources or 'upenn' in args.sources:
        if args.enter_passwords:
            url = getpass.getpass('REDCAP URL: ')
            api_token = getpass.getpass('REDCAP API_TOKEN: ')
            upenn_url = getpass.getpass('UPENN REDCAP URL: ')
            upenn_api_token = getpass.getpass('UPENN API_TOKEN: ')
        else:
            url = 'https://redcap.partners.org/redcap'
            api_token = '*****'
            upenn_url = 'https://redcap.upenn.org/redcap'
            upenn_api_token = '*****'

        template_dict['lochness']['REDCAP'] = {}

        template_dict['lochness']['SECRETS'] = {}
        template_dict['lochness']["email_sender_pw"] = "*****"
        

        for study in args.studies:
            project_name = study[:-2]
            if 'upenn' in args.sources and 'redcap' in args.sources:
                study_dict = {f'redcap.{project_name}': [project_name],
                              'redcap.UPENN': ['UPENN']}
                template_dict['redcap.UPENN'] = {
                        'URL': upenn_url,
                        'API_TOKEN': {'UPENN': upenn_api_token}}
            elif 'upenn' in args.sources and 'redcap' not in args.sources:
                study_dict = {'redcap.UPENN': ['UPENN']}
                template_dict['redcap.UPENN'] = {
                        'URL': upenn_url,
                        'API_TOKEN': {'UPENN': upenn_api_token}}
            else:
                study_dict = {f'redcap.{project_name}': [project_name]}

            study_secrete = '**PASSWORD_TO_ENCRYPTE_PROTECTED_DATA**'
            template_dict['lochness']['REDCAP'][study] = study_dict
            template_dict['lochness']['SECRETS'][study] = study_secrete

            if 'redcap' in args.sources:
                # lower part of the keyring
                template_dict[f'redcap.{project_name}'] = {
                        'URL': url,
                        'API_TOKEN': {project_name: api_token}}


    if 'xnat' in args.sources:
        if args.enter_passwords:
            url = getpass.getpass('XNAT URL: ')
            username = getpass.getpass('XNAT USERNAME: ')
            password = getpass.getpass('XNAT PASSWORD: ')
        else:
            url = '*****'
            username = '*****'
            password = '*****'

        for study in args.studies:
            # lower part of the keyring
            template_dict[f'xnat.{study}'] = {
                'URL': url,
                'USERNAME': username,
                'PASSWORD': password}

    if 'SECRETS' not in template_dict['lochness'].keys():
        template_dict['lochness']['SECRETS'] = {}

    if 'box' in args.sources:
        if args.enter_passwords:
            client_id = getpass.getpass('BOX CLIENT ID: ')
            client_secret = getpass.getpass('BOX CLIENT SECRET: ')
            enterprise_id = getpass.getpass('BOX ENTERPRISE ID: ')
        else:
            client_id = '*****'
            client_secret = '*****'
            enterprise_id = '*****'

        for study in args.studies:
            template_dict['lochness']['SECRETS'][study] = 'LOCHNESS_SECRETS'

            # lower part of the keyring
            template_dict[f'box.{study}'] = {
                'CLIENT_ID': client_id,
                'CLIENT_SECRET': client_secret,
                'ENTERPRISE_ID': enterprise_id}

    if 'mediaflux' in args.sources:
        for study in args.studies:
            template_dict['lochness']['SECRETS'][study] = 'LOCHNESS_SECRETS'

            if args.enter_passwords:
                mediaflux_user = getpass.getpass('Mediaflux User: ')
                mediaflux_password = getpass.getpass('Mediaflux Password: ')
            else:
                mediaflux_user = '*****'
                mediaflux_password = '*****'

            # lower part of the keyring
            template_dict[f'mediaflux.{study}'] = {
                'HOST': 'mediaflux.researchsoftware.unimelb.edu.au',
                'PORT': '443',
                'TRANSPORT': 'https',
                'DOMAIN': 'local',
                'USER': mediaflux_user,
                'PASSWORD': mediaflux_password}

    if 'mindlamp' in args.sources:
        for study in args.studies:
            # lower part of the keyring
            if args.enter_passwords:
                mindlamp_url = getpass.getpass('Mindlamp URL: ')
                mindlamp_ak = getpass.getpass('Mindlamp Access key: ')
                mindlamp_sk = getpass.getpass('Mindlamp  Secret key: ')
            else:
                mindlamp_url = '*****'
                mindlamp_ak = '*****'
                mindlamp_sk = '*****'

            template_dict[f'mindlamp.{study}'] = {
                "URL": mindlamp_url,
                "ACCESS_KEY": mindlamp_ak,
                "SECRET_KEY": mindlamp_sk}

    if 'daris' in args.sources:
        for study in args.studies:
            # lower part of the keyring
            template_dict[f'daris.{study}'] = {
                "URL": "https://daris.researchsoftware.unimelb.edu.au",
                "TOKEN": "******",
                "PROJECT_CID": "******",
                }

    if args.lochness_sync_send:
        if args.s3:
            pass
        elif args.rsync:
            # lower part of the keyring
            template_dict['rsync'] = {
                'ID': "rsync_server_id",
                'SERVER': "rsync_server_ip",
                'PASSWORD': "rsync_server_password",
                'PHOENIX_PATH_RSYNC': "/rsync/server/phoenix/path"}
        else:
            # lower part of the keyring
            template_dict[f'lochness_sync'] = {
                "HOST": "phslxftp2.partners.org",
                "USERNAME": "USERNAME",
                "PASSWORD": "*******",
                "PATH_IN_HOST": "/PATH/IN/HOST",
                "PORT": "2222",
                }

    if args.lochness_sync_receive:
        # lower part of the keyring
        template_dict[f'lochness_sync'] = {
            "PATH_IN_HOST": "/PATH/IN/HOST",
            }

    with open(keyring_loc, 'w') as f:
        json.dump(template_dict, f,
                  sort_keys=False,
                  indent='  ',
                  separators=(',', ': '))


def create_config_template(config_loc: Path, args: object) -> None:
    '''Create config file template'''

    config_example = f'''keyring_file: {args.outdir}/.lochness.enc
phoenix_root: {args.outdir}/PHOENIX
project_name: ProNET
BIDS: True
pid: {args.outdir}/lochness.pid
stderr: {args.outdir}/lochness.stderr
stdout: {args.outdir}/lochness.stdout
poll_interval: {args.poll_interval}
ssh_user: {args.ssh_user}
ssh_host: {args.ssh_host}
mindlamp_days_to_pull: 100
pii_table: {args.pii_csv}
lochness_sync_history_csv: {args.lochness_sync_history_csv}
days_to_keep: 100
removed_df_loc: {args.outdir}/PHOENIX/removed_files.csv
removed_phoenix_root: {args.outdir}/track_removed_files_PHOENIX
redcap_id_colname: chric_record_id
redcap_consent_colname: chric_consent_date

'''

    if 'rpms' in args.sources:
        config_example += '''RPMS_PATH: /mnt/prescient/RPMS_incoming
RPMS_id_colname: subjectkey
RPMS_consent_colname: Consent
'''
    if 'redcap' in args.sources:
        config_example += '''redcap_id_colname: chric_record_id
redcap_consent_colname: chric_consent_date
'''


    if args.s3:
        if 'rpms' in args.sources:
            s3_lines = f'''AWS_BUCKET_NAME: prescient-test
AWS_BUCKET_ROOT: TEST_PHOENIX_ROOT_PRESCIENT'''
        else:
            s3_lines = f'''AWS_BUCKET_NAME: pronet-test
AWS_BUCKET_ROOT: TEST_PHOENIX_ROOT_PRONET'''
        config_example += s3_lines

    if args.s3_selective_sync:
        # eg)
        # s3_selective_sync: ['mri', 'actigraphy']
        config_example += \
                f"\ns3_selective_sync: [{','.join(args.s3_selective_sync)}]"

    
    if 'redcap' in args.sources:
        config_example += '\nredcap:'

        for study in args.studies:
            redcap_deidentify_lines = f'''
    {study}:
        deidentify: True
        data_entry_trigger_csv: {args.det_csv}
        update_metadata: True'''
            config_example += redcap_deidentify_lines

    if 'mediaflux' in args.sources:
        config_example += '\nmediaflux:'

        for study in args.studies:
            line_to_add = f'''
    {study}:
        namespace: /projects/proj-5070_prescient-1128.4.380/{study}
        delete_on_success: False
        file_patterns:
            actigraphy:
                - vendor: Insights
                  product: GENEActivQC
                  data_dir: {study}_Actigraphy
                  pattern: '*'
            eeg:
                   - product: eeg
                     data_dir: {study}_EEG
                     pattern: '*'
            mri:
                   - product: mri
                     data_dir: {study}_MRI
                     pattern: '*'
            interviews:
                   - product: open
                     data_dir: {study}_Interviews/OPEN
                     out_dir: open
                     pattern: '*'
                   - product: psychs
                     data_dir: {study}_Interviews/PSYCHS
                     out_dir: psychs
                     pattern: '*'
                   - product: transcripts
                     data_dir: {study}_Interviews/transcripts/Approved
                     out_dir: transcripts
                     pattern: '*'
              '''

            config_example += line_to_add

    if 'box' in args.sources:
        config_example += '\nbox:'
        for study in args.studies:
            line_to_add = f'''
    {study}:
        base: ProNET/{study}
        delete_on_success: False
        file_patterns:
            actigraphy:
                   - vendor: Activinsights
                     product: GENEActiv
                     data_dir: {study}_Actigraphy
                     pattern: '*.*'
            eeg:
                   - product: eeg
                     data_dir: {study}_EEG
                     pattern: '*.*'
            interviews:
                   - product: open
                     data_dir: {study}_Interviews/OPEN
                     out_dir: open
                     pattern: '*.*'
                   - product: psychs
                     data_dir: {study}_Interviews/PSYCHS
                     out_dir: psychs
                     pattern: '*.*'
                   - product: transcripts
                     data_dir: {study}_Interviews/transcripts/Approved
                     out_dir: transcripts
                     pattern: '*.txt'
             '''

            config_example += line_to_add

    line_to_add = f'''
hdd:
    module_name:
        base: /PHOENIX
admins:
    - {args.email}
sender: {args.email}
notify:
    __global__:
        - {args.email}

file_check_notify:
    __global__:
        - {args.email}
                '''
    config_example += line_to_add

    with open(config_loc, 'w') as f:
        f.write(config_example)


def create_example_meta_file_advanced(metadata: str,
                                      project_name: str,
                                      sources: List[str]) -> None:
    '''Create example meta files'''

    col_input_to_col_dict = {'xnat': 'XNAT',
                             'redcap': 'REDCap',
                             'box': 'Box',
                             'mindlamp': 'Mindlamp',
                             'mediaflux': 'Mediaflux',
                             'daris': 'Daris',
                             'rpms': 'RPMS',
                             'upenn': 'REDCap'}

    df = pd.DataFrame({
        'Active': [1],
        'Consent': '1988-09-16',
        'Subject ID': 'subject01'})

    for source in sources:
        source_col = col_input_to_col_dict[source]
        if source == 'xnat':
            value = f'xnat.{project_name}:subproject:subject01'
        elif source_col == 'REDCap':
            if 'REDCap' in df.loc[0]:
                prev_value = f'{df.loc[0]};'
            else:
                prev_value = ''

            if source == 'upenn':
                value = prev_value + 'redcap.UPENN:subject01'
            else:
                value = prev_value + f'redcap.{project_name}:subject01'
        else:
            value = f'{source}.{project_name}:subject01'
        df.loc[0, source_col] = value

    return
    df.to_csv(metadata, index=False)


def get_arguments():
    '''Get arguments'''
    parser = ap.ArgumentParser(description='Lochness template maker')
    parser.add_argument('-o', '--outdir',
                        required=True,
                        help='Path of the Lochness template')
    parser.add_argument('-s', '--studies',
                        required=True,
                        nargs='+',
                        help='Names of studies')
    parser.add_argument('-ss', '--sources',
                        required=True,
                        nargs='+',
                        help='List of sources, eg) xnat, redcap, box, '
                             'mindlamp, mediaflux, etc.')
    parser.add_argument('-e', '--email',
                        required=True,
                        help='Email address')
    parser.add_argument('-p', '--poll_interval',
                        default=86400,
                        help='Poll interval in seconds')
    parser.add_argument('-sh', '--ssh_host',
                        required=True,
                        help='ssh id')
    parser.add_argument('-su', '--ssh_user',
                        required=True,
                        help='ssh id')
    parser.add_argument('-lss', '--lochness_sync_send',
                        default=True,
                        action='store_true',
                        help='Enable lochness to lochness transfer on the '
                             'sender side')
    parser.add_argument('--rsync',
                        default=False,
                        action='store_true',
                        help='Use rsync in lochness to lochness transfer')
    parser.add_argument('--s3',
                        default=False,
                        action='store_true',
                        help='Use s3 rsync in lochness to lochness transfer')
    parser.add_argument('--s3_selective_sync',
                        default=False,
                        nargs='+',
                        help='List of dtypes from protected root to transfer '
                             'using s3 rsync')
    parser.add_argument('-lsr', '--lochness_sync_receive',
                        default=False,
                        action='store_true',
                        help='Enable lochness to lochness transfer on the '
                             'server side')
    parser.add_argument('-lsh', '--lochness_sync_history_csv',
                        default='lochness_sync_history.csv',
                        help='Lochness sync history database csv path')
    parser.add_argument('-det', '--det_csv',
                        default='data_entry_trigger.csv',
                        help='Redcap data entry trigger database csv path')
    parser.add_argument('-pc', '--pii_csv',
                        default='pii_convert.csv',
                        help='Location of table to be used in deidentifying '
                             'redcap fields')
    parser.add_argument('-ep', '--enter_passwords',
                        action='store_true',
                        default=False,
                        help='Enter passwords to the shell to be stored in '
                             'the lochness.json')

    args = parser.parse_args()

    if Path(args.outdir).is_dir():
        sys.exit(f'*{args.outdir} already exists. Please provide another path')


    create_lochness_template(args)


if __name__ == '__main__':
    get_arguments()
