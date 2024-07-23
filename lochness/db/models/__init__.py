"""
Contains DB models for Lochness.
"""

from pathlib import Path
from typing import List, Union

from lochness import db
from lochness.db.models.study import Study
from lochness.db.models.subject import Subject
from lochness.db.models.phoenix_files import PhoenixFiles
from lochness.db.models.remote_files import RemoteFile
from lochness.db.models.file_mappings import FileMapping


def flatten_list(coll: list) -> list:
    """
    Flattens a list of lists into a single list.

    Args:
        coll (list): List of lists.

    Returns:
        list: Flattened list.
    """
    flat_list = []
    for i in coll:
        if isinstance(i, list):
            flat_list += flatten_list(i)
        else:
            flat_list.append(i)
    return flat_list


def init_db(config_file: Path):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        config_file (Path): Path to the config file.
    """
    drop_queries_l: List[Union[str, List[str]]] = [
        FileMapping.drop_table_query(),
        RemoteFile.drop_table_query(),
        PhoenixFiles.drop_table_query(),
        Subject.drop_table_query(),
        Study.drop_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        Study.init_table_query(),
        Subject.init_table_query(),
        PhoenixFiles.init_table_query(),
        RemoteFile.init_table_query(),
        FileMapping.init_table_query(),
    ]

    drop_queries = flatten_list(drop_queries_l)
    create_queries = flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(
        config_file=config_file,
        queries=sql_queries,
    )
