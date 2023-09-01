import os
from lochness.xnat import sync_new
from lochness.config import load


class Subject():
    def __init__(self, subject):
        self.xnat = {f'xnat.Pronet{subject[:2]}': subject}
        self.protected_folder = subject
        self.study ='study'
        self.id ='id'


def test_sync_new():
    Lochness = load('/mnt/ProNET/Lochness/config.yml')
    subject = Subject('SL00005')
    dry = False
    sync_new(Lochness, subject, dry=dry)

    print(os.popen('tree').read())
    print(os.popen('rm -rf processed raw'))


def test_wrong_id():
    Lochness = load('/mnt/ProNET/Lochness/config.yml')
    subject = Subject('SL00000')
    dry = False
    sync_new(Lochness, subject, dry=dry)

    print(os.popen('tree').read())
    print(os.popen('rm -rf processed raw'))
