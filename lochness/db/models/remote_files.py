#!/usr/bin/env python
"""
RemoteFile Model
"""

from datetime import datetime
from typing import Dict

from lochness import db


class RemoteFile:
    """
    Represents a file on some remote file system.

    Attributes:
        file_path (Path): The path to the file.
    """

    def __init__(
        self,
        file_path: str,
        remote_name: str,
        hash_val: str,
        last_checked: datetime,
        remote_metadata: Dict[str, str],
    ):
        """
        Initialize a RemoteFile object.

        Args:
            file_path (Path): The path to the file.
            remote_name (str): The name of the remote system.
            hash_val (str): The hash value of the file,
                as provided by the remote system.
            last_checked (datetime): The last time the file was checked.
            remote_metadata (Dict[str, str]): Metadata about the file,
        """
        self.file_path = file_path
        self.remote_name = remote_name
        self.hash_val = hash_val
        self.last_checked = last_checked
        self.remote_metadata = remote_metadata

    def __str__(self):
        """
        Return a string representation of the RemoteFile object.
        """
        return f"RemoteFile({self.file_path}, {self.remote_name}, {self.last_checked})"

    def __repr__(self):
        """
        Return a string representation of the File object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'files' table.
        """
        sql_query = """
        CREATE TABLE remote_files (
            r_file_path TEXT NOT NULL,
            r_remote_name TEXT NOT NULL,
            r_hash_val TEXT,
            r_last_checked TIMESTAMP NOT NULL,
            r_remote_metadata JSONB,
            PRIMARY KEY (r_file_path, r_remote_name)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'remote_files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS remote_files CASCADE;
        """

        return sql_query

    @staticmethod
    def find_matches_by_hash_query(hash_val: str) -> str:
        """
        Return the SQL query to find matching remote_files by hash.
        """
        sql_query = f"""
        SELECT r_file_path, r_remote_name, r_hash_val, r_last_checked, r_remote_metadata
        FROM remote_files
        WHERE r_hash_val = '{hash_val}';
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the RemoteFile object into the 'remote_files' table.

        Returns:
            str: The SQL query.
        """

        file_path = db.santize_string(str(self.file_path))
        remote_name = db.santize_string(self.remote_name)
        hash_val = db.santize_string(self.hash_val)
        last_checked = db.santize_string(self.last_checked)

        metadata = db.sanitize_json(self.remote_metadata)

        sql_query = f"""
            INSERT INTO remote_files (
                r_file_path, r_remote_name, r_hash_val, r_last_checked, r_remote_metadata
            ) VALUES (
                '{file_path}', '{remote_name}', '{hash_val}', '{last_checked}', '{metadata}'
            ) ON CONFLICT (file_path, remote_name) UPDATE SET
                r_hash_val = excluded.r_hash_val,
                r_last_checked = excluded.r_last_checked,
                r_remote_metadata = excluded.r_remote_metadata;
        """

        return sql_query
