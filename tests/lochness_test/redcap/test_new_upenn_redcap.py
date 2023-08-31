import lochness
import json
from pathlib import Path
import sys
lochness_root = Path(lochness.__path__[0]).parent
scripts_dir = lochness_root / 'scripts'
test_dir = lochness_root / 'tests'
sys.path.append(str(scripts_dir))
sys.path.append(str(test_dir))
from test_lochness import Tokens
from lochness.redcap import post_to_redcap


def test_import():
    print(lochness.__file__)


def test_new_format():

    token = Tokens()
    api_key, api_url = token.read_token_or_get_input('redcap_fake')

    redcap_subject = 'YA01508'
    redcap_subject_sl = 'ya01508'

    digits = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    digits_str = [str(x) for x in digits]
    contains_logic = []
    for subject_id in [redcap_subject, redcap_subject_sl]:
        contains_logic += [
                f"contains([src_subject_id], '{subject_id}_{x}')"
                for x in digits_str]
        contains_logic += [
                f"contains([src_subject_id], '{subject_id}={x}')"
                for x in digits_str]

    record_query = {
        'token': api_key,
        'content': 'record',
        'format': 'json',
        'filterLogic': f"[src_subject_id] = '{redcap_subject}' or "
                       f"[src_subject_id] = '{redcap_subject_sl}' or "
                       f"{' or '.join(contains_logic)}"
    }

    # post query to redcap
    content = post_to_redcap(api_url,
                             record_query,
                             False)

    # check if response body is nothing but a sad empty array
    content_dict_list = json.loads(content)
    # print(content_dict_list)

    content = json.dumps(content_dict_list).encode('utf-8')
    print(content)
