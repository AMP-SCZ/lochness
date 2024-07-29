#!/usr/bin/env python
"""
Initialize the database with the schema defined in lochness.db.models
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "lochness-dev":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging

import lochness.config as config
from lochness.db import models

logger = logging.getLogger(__name__)
logargs = {
    "level": logging.DEBUG,
    "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
}
logging.basicConfig(**logargs)


if __name__ == "__main__":
    logger.info("Initializing database...")
    logger.debug(
        "This will drop all tables and recreate them. DO NOT RUN THIS IN PRODUCTION."
    )

    config_file = "/var/lib/prescient/soft/lochness-dev/scratch/config.yml"
    logger.info(f"Loading config file: {config_file}")
    lochness_config = config.load(path=config_file)

    logger.info("Initializing database...")
    models.init_db(lochness_config)

    logger.info("Done!")
