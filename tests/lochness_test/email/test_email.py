
import lochness
from lochness.xnat import sync
from lochness import config
from pathlib import Path

import sys
lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))
from test_lochness import Args, KeyringAndEncrypt, Tokens
from test_lochness import show_tree_then_delete, rmtree, config_load_test
from test_lochness import initialize_metadata_test
from lochness_create_template import create_lochness_template

from lochness.email import send_out_daily_updates
import pytest

@pytest.fixture
def args_and_Lochness_BIDS():
    args = Args('tmp_lochness')
    args.sources = ['box']
    create_lochness_template(args)
    keyring = KeyringAndEncrypt(args.outdir)
    information_to_add_to_metadata = {'box': {
        'subject_id': '1001',
        'source_id': 'LA123456'}}

    # for study in args.studies:
        # update box metadata
        # initialize_metadata_test('tmp_lochness/PHOENIX', study,
                                 # information_to_add_to_metadata)

    lochness_obj = config_load_test('tmp_lochness/config.yml', '')

    # change protect to true for all actigraphy
    for study in args.studies:
        # new_list = []
        lochness_obj['box'][study]['base'] = 'PronetLA'

    return args, lochness_obj


def test_box_sync_module_no_redownload(args_and_Lochness_BIDS):
    args, Lochness = args_and_Lochness_BIDS
    Lochness['sender'] = 'kevincho.lochness@gmail.com'

    token = Tokens()
    (email_sender_pw,) = token.read_token_or_get_input('email')
    Lochness['keyring']['lochness']['email_sender_pw'] = email_sender_pw
    send_out_daily_updates(Lochness)
