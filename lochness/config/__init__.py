"""
Module to read Lochness configuration file and keyring file.
"""

import getpass as gp
import logging
import os
import string
from typing import Any, Dict

import cryptease as crypt
import yaml
import yaml.reader

logger = logging.getLogger(__name__)


def load(path: str, archive_base=None) -> Dict[str, Any]:
    """
    Load configuration file and keyring

    Uses passphrase from environment variable NRG_KEYRING_PASS if available.
    Otherwise, prompts user for passphrase.

    Args:
        path (str): path to configuration file (yaml)
        archive_base (str): path to the root of the archive

    Returns:
        Dict[str, Any]: configuration dictionary
    """
    logger.debug("loading configuration")
    Lochness = _read_config_file(path)

    if archive_base:
        Lochness["phoenix_root"] = archive_base
    if "phoenix_root" not in Lochness:
        raise ConfigError(
            "need either --archive-base or 'phoenix_root' in config file"
        )
    Lochness["phoenix_root"] = os.path.expanduser(Lochness["phoenix_root"])
    Lochness["keyring_file"] = os.path.expanduser(Lochness["keyring_file"])

    # box file pattern strings from the config to string template
    # regardless of the selected study in the args
    if "box" in Lochness:
        for _, study_dict in Lochness["box"].items():
            for _, modality_values in study_dict["file_patterns"].items():
                for modality_dict in modality_values:
                    modality_dict["pattern"] = string.Template(modality_dict["pattern"])

    with open(Lochness["keyring_file"], "rb") as fp:
        logger.info(f"reading keyring file {Lochness["keyring_file"]}")
        if "NRG_KEYRING_PASS" in os.environ:
            load.passphrase = os.environ["NRG_KEYRING_PASS"]
        if load.passphrase is None:
            load.passphrase = gp.getpass("enter passphrase: ")
        key = crypt.key_from_file(fp, load.passphrase)
        content = b""
        for chunk in crypt.decrypt(fp, key):
            content += chunk
        try:
            Lochness["keyring"] = yaml.load(content, Loader=yaml.FullLoader)
        except yaml.reader.ReaderError:
            raise KeyringError(
                f"could not decrypt keyring {Lochness["keyring_file"]} (wrong passphrase?)"
            )

    return Lochness


load.passphrase = None


class KeyringError(Exception):
    """
    Generic keyring error.
    """


def _read_config_file(path: str) -> Dict[str, Any]:
    """helper to read lochness configuration file"""

    expanded_path = os.path.expanduser(path)
    with open(expanded_path, "rb") as fp:
        try:
            cfg = yaml.load(fp.read(), Loader=yaml.FullLoader)
        except Exception as e:
            raise ConfigError(f"failed to parse {expanded_path} with error: {e}")
        return cfg


class ConfigError(Exception):
    """
    Malformed configuration file.
    """
