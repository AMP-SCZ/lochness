#!/usr/bin/env python
"""
File Model
"""

from pathlib import Path
from datetime import datetime

from lochness import db


class PhoenixFiles:
    """
    Represents a file on the PHOENIX file system.

    Attributes:
        file_path (Path): The path to the file.
    """

    def __init__(self, file_path: Path, with_hash: bool = True):
        """
        Initialize a File object.

        Automatically computes the file size and modification time.

        Args:
            file_path (Path): The path to the file.
        """
        self.file_path = file_path

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self.file_name = file_path.name
        self.file_type = file_path.suffix

        self.file_size_mb = file_path.stat().st_size / 1024 / 1024
        self.m_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        if with_hash:
            self.md5 = db.compute_hash(file_path=file_path, hash_type="md5")
        else:
            self.md5 = None

    def __str__(self):
        """
        Return a string representation of the File object.
        """
        return f"PhoenixFile({self.file_name}, {self.file_type}, {self.file_size_mb}, \
            {self.file_path}, {self.m_time}, {self.md5})"

    def __repr__(self):
        """
        Return a string representation of the File object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'phoenix_files' table.
        """
        sql_query = """
        CREATE TABLE phoenix_files (
            p_file_name TEXT NOT NULL,
            p_file_type TEXT NOT NULL,
            p_file_size_mb FLOAT NOT NULL,
            p_file_path TEXT PRIMARY KEY,
            m_time TIMESTAMP NOT NULL,
            md5 TEXT
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'phoenix_files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS phoenix_files CASCADE;
        """

        return sql_query

    @staticmethod
    def find_matches_by_hash_query(hash_val: str) -> str:
        """
        Return the SQL query to find matching files by hash.
        """
        sql_query = f"""
        SELECT p_file_name, p_file_type, p_file_size_mb, p_file_path, m_time, md5
        FROM phoenix_files
        WHERE md5 = '{hash_val}';
        """

        return sql_query

    @staticmethod
    def update_file_query(orig_path: Path, new_path: Path) -> str:
        """
        Return the SQL query to update the p_file_path of a File object.
        """
        orig_path = db.santize_string(str(orig_path))
        new_path = db.santize_string(str(new_path))

        sql_query = f"""
        UPDATE files
        SET p_file_path = '{new_path}'
        WHERE p_file_path = '{orig_path}';
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the File object into the 'phoenix_files' table.
        """
        f_name = db.santize_string(self.file_name)
        f_path = db.santize_string(str(self.file_path))

        if self.md5 is None:
            hash_val = "NULL"
        else:
            hash_val = self.md5

        sql_query = f"""
        INSERT INTO phoenix_files (p_file_name, p_file_type, p_file_size_mb,
            p_file_path, m_time, md5)
        VALUES ('{f_name}', '{self.file_type}', '{self.file_size_mb}',
            '{f_path}', '{self.m_time}', '{hash_val}')
        ON CONFLICT (p_file_path) DO UPDATE SET
            p_file_name = excluded.p_file_name,
            p_file_type = excluded.p_file_type,
            p_file_size_mb = excluded.p_file_size_mb,
            m_time = excluded.m_time,
            md5 = excluded.md5;
        """

        sql_query = db.handle_null(sql_query)

        return sql_query
