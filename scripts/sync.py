#!/usr/bin/env python

import os
import sys
import time
import lochness
import logging
import importlib
import pandas as pd
import argparse as ap
from pathlib import Path
import lochness.config as config
import lochness.daemon as daemon
import lochness.hdd as HDD
import lochness.xnat as XNAT
import lochness.redcap as REDCap
from lochness.redcap import save_redcap_metadata
import lochness.mindlamp as Mindlamp
import lochness.dropbox as Dropbox
import lochness.box as Box
import lochness.mediaflux as Mediaflux
import lochness.daris as Daris
import lochness.rpms as RPMS
import lochness.scheduler as scheduler
import lochness.icognition as iCognition
import lochness.onlinescoring as OnlineScoring
from lochness.transfer import lochness_to_lochness_transfer_sftp
from lochness.transfer import lochness_to_lochness_transfer_rsync
from lochness.transfer import lochness_to_lochness_transfer_s3
from lochness.transfer import lochness_to_lochness_transfer_s3_protected
from lochness.transfer import create_s3_transfer_table
from lochness.transfer import lochness_to_lochness_transfer_receive_sftp
from lochness.email import send_out_daily_updates
from datetime import datetime, date
from lochness.cleaner import rm_transferred_files_under_phoenix
from lochness.utils.source_check import check_source
# import dpanonymize

SOURCES = {
    'xnat': XNAT,
    'redcap': REDCap,
    'mindlamp': Mindlamp,
    'dropbox': Dropbox,
    'box': Box,
    'mediaflux': Mediaflux,
    'daris': Daris,
    'rpms': RPMS,
    'icognition': iCognition,
    'onlinescoring': OnlineScoring,
    'upenn': REDCap,
}

DIR = os.path.dirname(__file__)

logger = logging.getLogger(os.path.basename(__file__))


def sync_lock(args, Lochness) -> None:
    sync_lock = Path(Lochness['phoenix_root']) / \
            'sync_lock_file_history.csv'

    logger.info('Loading sync lock history db')
    if sync_lock.is_file():
        sync_lock_df = pd.read_csv(sync_lock)
    else:
        sync_lock_df = pd.DataFrame(
                columns=['file_path',
                         'file_modified_at',
                         'sync_lock_released_at'])

    while True:
        file_list = Path(args.sync_after_update_in_file).parent.glob(
                Path(args.sync_after_update_in_file).name)

        for file_path in file_list:
            logger.info(f'checking if {file_path} was seen before')
            if str(file_path) in sync_lock_df.file_path.tolist():
                logger.info(f'Sleep - seen this file before')
                continue
            else:
                logger.info(f'New file!')
                sync_lock_df_tmp = pd.DataFrame({
                    'file_path': [file_path],
                    'file_modified_at': file_path.stat().st_mtime,
                    'sync_lock_released_at': datetime.now()
                    })
                sync_lock_df = pd.concat([sync_lock_df,
                                          sync_lock_df_tmp])
                sync_lock_df.to_csv(sync_lock)

                return

        time.sleep(1800)


def main():
    parser = ap.ArgumentParser(description='PHOENIX data syncer')
    parser.add_argument('-c', '--config', required=True,
                        help='Configuration file')
    parser.add_argument('-a', '--archive-base',
                        help='Base output directory')
    parser.add_argument('--dry', action='store_true',
                        help='Dry run')
    parser.add_argument('--skip-inactive', action='store_true',
                        help='Skip inactive subjects')
    parser.add_argument('-l', '--log-file',
                        help='Log file')
    parser.add_argument('--hdd', nargs='+', default=[],
                        help='choose hdds to sync')
    parser.add_argument('--source', nargs='+', choices=SOURCES.keys(),
                        default=SOURCES.keys(), metavar='',
                        help='Sources to sync {%(choices)s}')
    parser.add_argument('--continuous', action='store_true',
                        help='Continuously download data')
    parser.add_argument('--studies', nargs='+', default=[],
                        help='Studies to sync')
    parser.add_argument('--subject', nargs='+', default=[],
                        help='Subjects to sync')
    parser.add_argument('--fork', action='store_true',
                        help='Daemonize the process')
    parser.add_argument('--until', type=scheduler.parse,
                        help='Pause execution until specified date e.g., '
                             '2017-01-01T15:00:00')
    parser.add_argument('-lss', '--lochness_sync_send',
                        action='store_true',
                        default=False,
                        help='Enable lochness to lochness transfer on the '
                             'sender side')
    parser.add_argument('-rsync', '--rsync',
                        action='store_true',
                        default=False,
                        help='Use rsync in lochness to lochness transfer')
    parser.add_argument('-s3', '--s3',
                        action='store_true',
                        default=False,
                        help='Use s3 bucket in lochness to lochness transfer')
    parser.add_argument('-lsr', '--lochness_sync_receive',
                        action='store_true',
                        default=False,
                        help='Enable lochness to lochness transfer on the '
                             'server side')
    parser.add_argument('-ds', '--daily_summary', action='store_true',
                        help='Enable daily summary email function')
    parser.add_argument('-cs', '--check_source', action='store_true',
                        help='Enable check source email function')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug messages')
    parser.add_argument('-rof', '--remove_old_files',
                        action='store_true',
                        help='Remove old files which are already transferred '
                             'to s3 from PHOENIX directory')
    parser.add_argument('-sa', '--sync_after_update_in_file',
                        type=str,
                        help='Path patterns to look for a new file for a sync '
                             'to start')
    args = parser.parse_args()

    # configure logging for this application
    lochness.configure_logging(logger, args)

    # replace args.hdd with corresponding lochness.hdd modules
    if args.hdd:
        args.hdd = [HDDS.get(x) for x in args.hdd]

    # replace args.source with corresponding lochness modules
    if args.source:
        args.input_sources = args.source
        args.source = list(set([SOURCES[x] for x in args.source]))
    else:
        args.input_sources = []

    # load the lochness configuration file and keyring
    Lochness = config.load(args.config, args.archive_base)

    # register log-file path
    Lochness['log_file'] = str(args.log_file)

    # fork the current process if necessary
    if args.fork:
        logger.info('forking the current process')
        daemon.daemonize(Lochness['pid'], stdout=Lochness['stdout'],
                         stderr=Lochness['stderr'], wdir=os.getcwd())

    # pause execution until
    if args.until:
        until = datetime.strptime(args.until, '%Y-%m-%dT%H:%M:%S')
        logger.info('pausing execution until {0}'.format(until))
        scheduler.until(until)


    # run downloader once, or continuously
    if args.continuous:
        while True:
            if args.sync_after_update_in_file:
                logger.info('Sync lock option is on')
                sync_lock(args, Lochness)

            # remove already transferred files
            if args.remove_old_files:
                rm_transferred_files_under_phoenix(
                        Lochness['phoenix_root'],
                        days_to_keep=Lochness['days_to_keep'],
                        removed_df_loc=Lochness['removed_df_loc'],
                        removed_phoenix_root=Lochness['removed_phoenix_root'])

            logger.info("Running sync from script/sync.py")
            logger.info(f"args: {args}")
            do(args, Lochness)

            email_dates_file = Path(Lochness['phoenix_root']).parent / \
                    '.email_tmp.txt'
            if email_dates_file.is_file():
                with open(email_dates_file, 'r') as fp:
                    dates_email_sent = [x.strip() for x in fp.readlines()]
            else:
                dates_email_sent = []

            # daily email
            if args.daily_summary and \
                    str(date.today()) not in dates_email_sent:

                if datetime.today().isoweekday() in [6, 7]:  # Weekends
                    pass  # no email
                elif datetime.today().isoweekday() == 1:  # Monday
                    days_to_summarize = 3
                    send_out_daily_updates(Lochness, days=days_to_summarize)
                    if args.check_source:
                        check_source(Lochness)
                else:
                    send_out_daily_updates(Lochness)
                    if args.check_source:
                        check_source(Lochness)

                with open(email_dates_file, 'w') as fp:
                    fp.write(str(date.today()))

            poll_interval = int(Lochness['poll_interval'])
            logger.info(f'sleeping for {poll_interval} seconds')
            time.sleep(Lochness['poll_interval'])
    else:
        # remove already transferred files
        if args.remove_old_files:
            rm_transferred_files_under_phoenix(
                    Lochness['phoenix_root'],
                    days_to_keep=Lochness['days_to_keep'],
                    removed_df_loc=Lochness['removed_df_loc'],
                    removed_phoenix_root=Lochness['removed_phoenix_root'])

        do(args, Lochness)

        # email
        if args.daily_summary:
            # check_source(Lochness)
            if args.check_source:
                check_source(Lochness)


def do(args, Lochness):
    # Lochness to Lochness transfer on the receiving side
    if args.lochness_sync_receive:
        lochness_to_lochness_transfer_receive_sftp(Lochness)
        return True  # break the do function here for the receiving side

    # initialize (overwrite) metadata.csv using either REDCap or RPMS database
    if 'redcap' in args.input_sources or 'rpms' in args.input_sources:
        upenn_redcap = True if 'upenn' in args.input_sources else False
        # for ProNET and PRESCIENT, single REDCap and RPMS repo has
        # information from multiple site
        multiple_site = True if len(args.studies) > 1 else False

        lochness.initialize_metadata(Lochness, args,
                                     multiple_site, upenn_redcap)

    logger.info(f"Studies: {' '.join(args.studies)}")
    
    n = 0
    for subject in lochness.read_phoenix_metadata(Lochness, args.studies):
        if n == 0:
            save_redcap_metadata(Lochness, subject)

        if args.subject:
            if subject.id not in args.subject:
                n += 1
                continue

        if not subject.active and args.skip_inactive:
            logger.info(f'skipping inactive subject={subject.id}, '
                        f'study={subject.study}')
            continue

        if args.hdd:
            for Module in args.hdd:
                lochness.attempt(Module.sync, Lochness, subject, dry=args.dry)
        else:
            for Module in args.source:
                lochness.attempt(Module.sync, Lochness, subject, dry=args.dry)
        n += 1

    # anonymize PII

    #if Lochness['s3_selective_sync']:
    #    dpanonymize.lock_lochness(
    #            Lochness,
    #            pii_table_loc=Lochness['pii_table'],
    #            s3_selective_sync = Lochness['s3_selective_sync'])
    #else:
    #    dpanonymize.lock_lochness(
    #            Lochnesss, pii_table_loc=Lochness['pii_table'])

    # transfer new files after all sync attempts are done
    if args.lochness_sync_send:
        if args.s3:
            # for data under GENERAL
            lochness_to_lochness_transfer_s3(Lochness,
                                             args.studies,
                                             args.input_sources)

            # for data under PROTECTED (for selected datatypes)
            if 's3_selective_sync' in Lochness:
                lochness_to_lochness_transfer_s3_protected(Lochness,
                                                           args.studies,
                                                           args.input_sources)

            # save details of transferred files under PHOENIX/s3_log.csv
            create_s3_transfer_table(Lochness)
                
        elif args.rsync:
            lochness_to_lochness_transfer_rsync(Lochness)
        else:
            lochness_to_lochness_transfer_sftp(Lochness)



if __name__ == '__main__':
    main()
