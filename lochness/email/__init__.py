import re
import os
from pathlib import Path
import string
import smtplib
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, date, timedelta
from typing import List
import getpass
import subprocess
import pandas as pd
from pytz import timezone
tz = timezone('EST')

__dir__ = os.path.dirname(__file__)


def send(recipients, sender, subject, message):
    '''send an email'''
    email_template = os.path.join(__dir__, 'template.html')
    with open(email_template, 'r') as fo:
        template = string.Template(fo.read())
    message = template.safe_substitute(message=str(message))
    msg = MIMEText(message, 'html')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()


def send_detail(Lochness,
                sender,
                recipients_for_each_study: dict,
                title: str, subtitle: str, first_message: str,
                second_message: str, code: List[str],
                in_mail_footer: str,
                test: bool = False,
                mailx: bool = True,
                recipients: list = []) -> None:
    '''Email Lochness updates
    
    This function uses Linux's mailx system by default. But when
    mailx = False, it uses Google's SMTP server to send emails.
    For the latter case, you will need to set up a
    Google account with "Less secure app access" turned on, from "Manage
    your Google account" page. Also, the keyring file should have email
    password at the same level as the "SECRETS" field.

        {...,
        "SECRETS": {"PronetLA": "LOCHNESS_SECRETS"},
        "email_sender_pw": "PASSWORD"
        }

    The configuration file should have
        sender: senderemail@gmail.com
        notify:
          __global__:
              - receiver1@anyemail.com
              - receiver2@anyemail.com

    
    '''
    if recipients == []:
        for study, study_recipients in recipients_for_each_study.items():
            for recipient in study_recipients:
                if recipients not in recipients:
                    recipients.append(recipient)


    email_template_dir = os.path.join(__dir__)
    env = Environment(loader=FileSystemLoader(str(email_template_dir)))
    template = env.get_template('bootdey_template.html')

    footer = f'If you see any error, please email {sender}'

    server_name = Lochness['project_name'] \
        if 'project_name' in Lochness else 'Data aggregation server'

    if Lochness.get('production', False):
        title = f'{server_name} Production: {title} {datetime.now(tz).date()}'
    else:
        title = f'{server_name}: {title} {datetime.now(tz).date()}'


    html_str = template.render(title=title,
                               subtitle=subtitle,
                               first_message=first_message,
                               second_message=second_message,
                               code=code,
                               in_mail_footer=in_mail_footer,
                               footer=footer,
                               server=server_name,
                               username=getpass.getuser())

    msg = MIMEText(html_str, 'html')
    msg['Subject'] = title
    msg['From'] = sender
    msg['To'] = recipients[0]

    if mailx:
        s = smtplib.SMTP('localhost')
    else:
        sender_pw = Lochness['keyring']['lochness']['email_sender_pw']
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(sender, sender_pw)

    if not test:
        s.sendmail(sender, recipients, msg.as_string())
        print('Email sent')
    else:
        print(html_str)

    s.quit()


def send_out_daily_updates(Lochness, days: int = 1,
                           test: bool = False, mailx: bool = True,
                           shorten: bool = True):
    '''Send daily updates from Lochness'''

    s3_log = Path(Lochness['phoenix_root']) / 's3_log.csv'
    if s3_log.is_file():
        s3_df = pd.read_csv(s3_log)
        s3_df['timestamp'] = pd.to_datetime(s3_df['timestamp'])
        s3_df['ctime'] = pd.to_datetime(s3_df['ctime'], format='mixed').apply(
                lambda x: x.replace(microsecond=0))

        # get the timestamp to check the dataflow from
        date_to_check_from = datetime.now(tz).date() - timedelta(days=days-1)
        timestamp_to_check_from = pd.Timestamp(date_to_check_from)

        s3_df_selected = s3_df[s3_df['timestamp'] > timestamp_to_check_from]
        s3_df_selected = s3_df_selected.fillna('_')

        s3_df_selected = s3_df_selected[~s3_df_selected.filename.str.contains(
            'metadata.csv')][['timestamp', 'filename', 'protected', 'study',
                              'processed', 'subject', 'datatypes', 'ctime']]

        s3_df_selected['date'] = s3_df_selected['timestamp'].apply(
                lambda x: x.date())

        # remove logfile
        s3_df_selected = s3_df_selected[s3_df_selected.filename != '.log']
        count_df = s3_df_selected.groupby([
            'date', 'protected', 'study',
            'processed', 'datatypes']).count()[['filename']]
        count_df.columns = ['file count']
        count_df = count_df.reset_index()
        count_df.columns = ['Transfer date', 'Protected vs General', 'Site',
                'Raw vs Processed', 'Data type',
                'Number of files transferred']
        s3_df_selected.drop('date', axis=1, inplace=True)

    else:
        s3_df_selected = pd.DataFrame()
        count_df = pd.DataFrame()

    list_of_lines_from_tree = ['']

    day_days = 'days' if days > 1 else 'day'

    if len(s3_df_selected) == 0:
        send_detail(
            Lochness,
            Lochness['sender'],
            Lochness['notify'],
            'Lochness', f'Daily updates {datetime.now(tz).date()}',
            'There is no update!', '',
            list_of_lines_from_tree,
            '',
            test, mailx)
    else:
        s3_df_selected.columns = ['Transfer time (UTC)', 'File name',
                                  'Protected', 'Study', 'Processed', 'Subject',
                                  'Datatype', 'Download time (UTC)']
        s3_df_selected.reset_index().drop('index', axis=1, inplace=True)

        # too many dicom file names -> remove
        subject_mri_gb = s3_df_selected[
               (s3_df_selected.Datatype == 'mri') &
               (~s3_df_selected['File name'].str.startswith('.')) &
               (~s3_df_selected['File name'].str.endswith('.gif'))
                ].groupby('Subject')

        sample_mri_df = pd.DataFrame()
        for _, table in subject_mri_gb:
            if len(table) > 2:
                # select only two raws
                subject_mri_sample = table.iloc[:2].copy()
                subject_mri_sample.loc[
                        subject_mri_sample.index[-1], 'File name'] = '...'
                sample_mri_df = pd.concat([sample_mri_df, subject_mri_sample])
            else:
                sample_mri_df = pd.concat([sample_mri_df, table])

        s3_df_selected = pd.concat([
            s3_df_selected[s3_df_selected.Datatype != 'mri'],
            sample_mri_df]).sort_index().reset_index(drop=True)

        in_mail_footer = 'Only the files transferred to NDA are shown.'

        if shorten:
            s3_df_tmp = s3_df_selected.copy()
            for index, row in s3_df_tmp.iterrows():
                s3_df_tmp.loc[index, 'File name'] = re.sub(
                        row['Subject'], '{SUBJECT_ID}', row['File name'])

                mindlamp_act_patt = r'_activity_(\d{4}_\d{2}_\d{2}.+)$'
                if re.search(mindlamp_act_patt, row['File name']):
                    rest_str = re.search(mindlamp_act_patt,
                                         row['File name']).group(1)
                    s3_df_tmp.loc[index, 'File name'] = \
                            f'SUBJECT_activity_{rest_str}'

                for patt in ['-diaryTranscriptQC-day', '_audioJournal_day',
                        '-diaryAudioQC-day']:
                    if re.search(patt,
                                 row['File name']):
                        s3_df_tmp.loc[index, 'File name'] = \
                                re.search(r'([a-zA-Z]+)[-_]day', patt).group(1)

            gb = s3_df_tmp.groupby('File name')
            for unique_filename, table in gb:
                s3_df_tmp.loc[table.index, 'Subject'] = \
                    f'{len(table)} subjects'
                s3_df_tmp.loc[table.index, 'Transfer time (UTC)'] = \
                        table['Transfer time (UTC)'].iloc[0]
                s3_df_tmp.loc[table.index, 'Download time (UTC)'] = \
                        table['Download time (UTC)'].iloc[0]
            s3_df_selected = s3_df_tmp.drop_duplicates()
            rename_Subject = lambda x: 'Number of subjects' if x == 'Subject' \
                    else x
            s3_df_selected.columns = [rename_Subject(x) for x in
                                      s3_df_selected.columns]

        send_detail(
            Lochness,
            Lochness['sender'],
            Lochness['notify'],
            'Lochness', f'Daily updates {datetime.now(tz).date()} '
                        f'(for the past {days} {day_days})',
            '<h2>Summary of the files transferred to NDA</h2>' \
                    + count_df.to_html() + '<br>',
            '<h2>More details for each data type</h2>' \
                    + '<br>'.join(
                        [f"<h3>{datatype.upper()}</h3>{table.to_html()}" for 
                            datatype, table in 
                            s3_df_selected.groupby('Datatype')]),
            list_of_lines_from_tree,
            in_mail_footer, test, mailx)


def attempts_error(Lochness, attempt):
    msg = '\n'.join(attempt.warnings)
    send(Lochness['admins'], Lochness['sender'], 'error report', msg)


def metadata_error(Lochness, message):
    send(Lochness['admins'], Lochness['sender'], 'bad metadata', message)

