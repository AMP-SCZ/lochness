import os
import subprocess
import yaml
import uuid
import xnat
import yaxil
import shutil
import logging
import lochness
import tempfile as tf
import collections as col
from pathlib import Path
import lochness.net as net
import lochness.tree as tree
import lochness.config as config
from lochness.cleaner import is_transferred_and_removed

yaml.SafeDumper.add_representer(
        col.OrderedDict, yaml.representer.SafeRepresenter.represent_dict)

logger = logging.getLogger(__name__)


@net.retry(max_attempts=5)
def sync_old(Lochness, subject, dry=False):
    logger.debug('exploring {0}/{1}'.format(subject.study, subject.id))

    for alias, xnat_uids in iter(subject.xnat.items()):
        Keyring = Lochness['keyring'][alias]
        auth = yaxil.XnatAuth(url=Keyring['URL'], username=Keyring['USERNAME'],
                              password=Keyring['PASSWORD'])
        '''
        pull XNAT data agnostic to the case of subject IDs loop over lower and
        upper case IDs if the data for one ID do not exist, experiments(auth,
        xnat_uid) returns nothing preventing the execution of inner loop
        '''
        _xnat_uids = xnat_uids + [(x[0], x[1].lower()) for x in xnat_uids]
        for xnat_uid in _xnat_uids:
            for experiment in experiments(auth, xnat_uid):
                logger.info(experiment)
                dirname = tree.get('mri',
                                   subject.protected_folder,
                                   processed=False,
                                   BIDS=Lochness['BIDS'],
                                   makedirs=True)
                dst = os.path.join(dirname, experiment.label.upper())

                # do not re-download already transferred & removed data
                if is_transferred_and_removed(Lochness, dst):
                    continue

                if os.path.exists(dst):
                    try:
                        check_consistency(dst, experiment)
                        continue
                    except ConsistencyError as e:
                        logger.warn(e)
                        message = 'A conflict was detected in study ' \
                                  f'{subject.study}\n' \
                                  f'There is existing data in {dst}, but ' \
                                  'it does not match the data on XNAT.' \
                                  'Please check MRI data saved in {dst} and ' \
                                  'compared to the XNAT data.'

                        lochness.notify(Lochness, message, study=subject.study)
                        # lochness.backup(dst)
                        continue

                message = 'downloading {PROJECT}/{LABEL} to {FOLDER}'
                logger.debug(message.format(PROJECT=experiment.project,
                                            LABEL=experiment.label,
                                            FOLDER=dst))

                if not dry:
                    tmpdir = tf.mkdtemp(dir=dirname, prefix='.')
                    os.chmod(tmpdir, 0o0755)
                    yaxil.download(auth, experiment.label,
                                   project=experiment.project,
                                   scan_ids=['ALL'], out_dir=dst,
                                   in_mem=False, attempts=3,
                                   out_format='native')
                    logger.debug('saving .experiment file')
                    save_experiment_file(tmpdir, auth.url, experiment)
                    os.rename(tmpdir, dst)


@net.retry(max_attempts=5)
def sync(Lochness, subject, dry=False):
    logger.debug('exploring {0}/{1}'.format(subject.study, subject.id))

    for alias, xnat_uids in iter(subject.xnat.items()):
        Keyring = Lochness['keyring'][alias]
        auth = yaxil.XnatAuth(url=Keyring['URL'], username=Keyring['USERNAME'],
                              password=Keyring['PASSWORD'])
        '''
        pull XNAT data agnostic to the case of subject IDs loop over lower and
        upper case IDs if the data for one ID do not exist, experiments(auth,
        xnat_uid) returns nothing preventing the execution of inner loop
        '''
        _xnat_uids = xnat_uids + [(x[0], x[1].lower()) for x in xnat_uids]
        for xnat_uid in _xnat_uids:
            for experiment in experiments(auth, xnat_uid):
                logger.info(experiment)
                dirname = tree.get('mri',
                                   subject.protected_folder,
                                   processed=False,
                                   BIDS=Lochness['BIDS'])
                dst = os.path.join(dirname, f'{experiment.label.upper()}.zip')

                # do not re-download already transferred & removed data
                if is_transferred_and_removed(Lochness, dst):
                    continue

                archieved_date = experiment.archived_date
                archieved_date_log = os.path.join(
                        dirname, f'.{experiment.label.upper()}')
                if os.path.exists(dst):
                    if Path(archieved_date_log).is_file():
                        with open(archieved_date_log, 'r') as fp:
                            archieved_date_prev = fp.read().strip()

                        if archieved_date == archieved_date_prev:
                            # same archieved date
                            continue

                message = 'downloading {PROJECT}/{LABEL} to {FOLDER}'
                logger.debug(message.format(PROJECT=experiment.project,
                                            LABEL=experiment.label,
                                            FOLDER=dst))

                if not dry:
                    yaxil.download(auth, experiment.label,
                                   project=experiment.project,
                                   scan_ids=['ALL'], out_file=dst,
                                   in_mem=False, attempts=3,
                                   out_format='native', extract=False)
                    save_experiment_file(dirname, auth.url, experiment)
                    with open(archieved_date_log, 'w') as fp:
                        fp.write(archieved_date)


class NoMatchingSubjectXNAT(Exception):
    pass


def set_TMPDIR(Lochness):
    try:
        tmp_dir = Lochness['tmp_dir']
    except KeyError:
        tmp_dir = os.environ.get('TMPDIR')
        if tmp_dir is None:
            tmp_dir = tf.gettempdir()

    # Set the TMPDIR environment variable to a default value
    os.environ['TMPDIR'] = tmp_dir

    return tmp_dir


def download_xnat_session_dataorc(
    host: str,
    project: str,
    subject: str,
    session: str,
    out_dir: str
) -> None:
    """
    Downloads session data from an XNAT server using the dataorc command-line tool.

    Args:
        host (str): The hostname of the XNAT server.
        project (str): The name of the project containing the session data.
        subject (str): The ID of the subject.
        session (str): The ID of the session to download.
        out_dir (str): The directory to which the session data should be downloaded.

    Returns:
        None
    """

    command_array = [
        '/opt/software/dataorc-bin/dataorc',
        'xnat-download-session',
        '--host', host,
        '--project', project,
        '--subject', subject,
        '--session', session,
        '--output-dir', out_dir
    ]

    command = ' '.join(command_array)

    logger.info(f'Executing command: {command}')

    try:
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to execute command: {command}')
        logger.error(f'Error message: {e}')
        raise e


@net.retry(max_attempts=5)
def sync_xnatpy(Lochness, subject, dry=False):
    """A new sync function with XNATpy"""
    logger.debug('exploring {0}/{1}'.format(subject.study, subject.id))

    tmp_dir = set_TMPDIR(Lochness)

    # remove xnatpy tmp files
    for tmp_file in Path(tmp_dir).glob('*generated_xnat.py'):
        os.remove(tmp_file)

    for tmp_file in Path(tmp_dir).glob('tmp_xnat*'):
        os.remove(tmp_file)

    for alias, xnat_uids in iter(subject.xnat.items()):
        keyring = Lochness['keyring'][alias]
        session = xnat.connect(keyring['URL'],
                               keyring['USERNAME'],
                               keyring['PASSWORD'])
        '''
        pull XNAT data agnostic to the case of subject IDs loop over lower and
        upper case IDs if the data for one ID do not exist, experiments(auth,
        xnat_uid) returns nothing preventing the execution of inner loop
        '''
        site = xnat_uids[0][1][:2]
        _xnat_uids = [(x[0], x[1].upper()) for x in xnat_uids] + \
                     [(x[0], x[1].lower()) for x in xnat_uids]

        for ses in session.projects:
            if 'pronet' in ses.lower():
                if site in ses:
                    break

        project = session.projects[ses]
        xnat_subject = ''
        for xnat_uid in _xnat_uids:
            try:
                xnat_subject = project.subjects[xnat_uid[1]]
                break
            except KeyError as e:
                continue

        if xnat_subject == '':
            msg = 'There is no matching subject in XNAT database: '
            msg += f"{' / '.join([x[1] for x in _xnat_uids])}"
            logger.debug(msg)
            continue

        for exp_id, experiment in xnat_subject.experiments.items():
            dirname = tree.get('mri',
                               subject.protected_folder,
                               processed=False,
                               BIDS=Lochness['BIDS'])
            dst = os.path.join(dirname, f'{experiment.label.upper()}.zip')

            if os.path.exists(dst):
                continue

            message = 'downloading {PROJECT}/{LABEL} to {FOLDER}'
            logger.debug(message.format(PROJECT=experiment.project,
                                        LABEL=experiment.label,
                                        FOLDER=dst))

            if not dry:
                #with tf.NamedTemporaryFile(dir=tmp_dir,
                #                           prefix='tmp_xnat_',
                #                           delete=False) as tmpfilename:
                #    experiment.download(tmpfilename.name)
                #    shutil.move(tmpfilename.name, dst)
                #    os.chmod(dst, 0o0755)

                with tf.TemporaryDirectory(dir=tmp_dir,
                                           prefix='tmp_xnat_') as tmpdirname:
                    download_xnat_session_dataorc(
                        host=keyring['URL'],
                        project=experiment.project,
                        subject=xnat_subject.label,
                        session=experiment.label,
                        out_dir=tmpdirname
                    )

                    downloaded_files = os.listdir(tmpdirname)

                    downloaded_file = downloaded_files[0]
                    downloaded_file_path = os.path.join(tmpdirname, downloaded_file)

                    shutil.move(downloaded_file_path, dst)
                    os.chmod(dst, 0o0755)


def check_consistency(d, experiment):
    '''check that local data still matches data in xnat'''
    experiment_file = os.path.join(d, '.experiment')
    if not os.path.exists(experiment_file):
        raise ConsistencyError('file not found {0}'.format(experiment_file))
    with open(experiment_file, 'r') as fo:
        experiment_local = yaml.load(fo.read(), Loader=yaml.FullLoader)
    local_uid = experiment_local['id']
    remote_uid = experiment.id
    if local_uid != remote_uid:
        raise ConsistencyError('conflict detected {0} != {1}'.format(
            local_uid, remote_uid))


class ConsistencyError(Exception):
    pass


def save_experiment_file(d, url, experiment, extract=False):
    '''save xnat experiment metadata to a file named .experiment'''
    if extract:
        experiment_file = os.path.join(d, '.experiment')
    else:
        experiment_file = os.path.join(
                d, f'{experiment.label.upper()}.experiment')

    blob = experiment._asdict()
    blob['source'] = url.rstrip('/') + '/'
    blob['uuid'] = str(uuid.uuid4())
    with tf.NamedTemporaryFile(dir=d, delete=False) as fo:
        content = yaml.safe_dump(blob, default_flow_style=False)
        fo.write(content.encode('utf-8'))
        fo.flush()
        os.fsync(fo.fileno())
    os.chmod(fo.name, 0o0644)
    os.rename(fo.name, experiment_file)


def experiments(auth, uid):
    '''generator for mr session ids'''
    try:
        project, subject = uid
        logger.info('searching xnat for {0}'.format(uid))
        xnat_subject = yaxil.subjects(auth, subject, project)
        xnat_subject = next(xnat_subject)
    except yaxil.exceptions.AccessionError as e:
        logger.info('Accession Error for {0}'.format(uid))
        return
    except yaxil.exceptions.NoSubjectsError as e:
        logger.info('no xnat subject registered for {0}'.format(uid))
        return

    for experiment in yaxil.experiments(auth, subject=xnat_subject):
        yield experiment

