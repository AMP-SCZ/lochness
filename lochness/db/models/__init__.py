"""
Contains DB models for Lochness.
"""

from typing import Any, Dict, List, Union

from lochness import db
from lochness.db.models.file_mappings import FileMapping
from lochness.db.models.files_audit_log import AuditLog
from lochness.db.models.phoenix_files import PhoenixFiles
from lochness.db.models.remote_files import RemoteFile
from lochness.db.models.study import Study
from lochness.db.models.subject import Subject


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


def init_db(lochness_config: Dict[str, Any]):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        lochness_config (Path): Path to the config file.
    """
    drop_queries_l: List[Union[str, List[str]]] = [
        AuditLog.drop_table_query(),
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
        AuditLog.init_table_query(),
    ]

    drop_queries = flatten_list(drop_queries_l)
    create_queries = flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(
        lochness_config=lochness_config,
        queries=sql_queries,
        show_commands=True,
    )
