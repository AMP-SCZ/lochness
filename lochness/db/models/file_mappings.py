"""
File mappings are used to map files from one source to another.

This module contains the FileMapping class, which represents a mapping between
a file on the local file system and a file on a remote file system.
"""

from pathlib import Path

from lochness import db


class FileMapping:
    """
    Maps a file on a remote file system to a file on the local file system.

    Attributes:
        remote_file_path (Path): The path to the file on the remote file system.
        local_file_path (Path): The path to the file on the local file system.
        remote_name (str): The name of the remote system.
        subject_id (str): The subject ID assciated with this asset.
        modality (str): The modality associated with this asset.
    """

    def __init__(
        self,
        remote_file_path: Path,
        local_file_path: Path,
        remote_name: str,
        subject_id: str,
        modality: str,
    ):
        """
        Initialize a FileMapping object.

        Args:
            remote_file_path (Path): The path to the file on the remote file system.
            local_file_path (Path): The path to the file on the local file system.
            remote_name (str): The name of the remote system.
            subject_id (str): The subject ID associated with this asset.
            modality (str): The modality associated with this asset.
        """
        self.remote_file_path = remote_file_path
        self.local_file_path = local_file_path
        self.remote_name = remote_name
        self.subject_id = subject_id
        self.modality = modality

    def __str__(self):
        """
        Return a string representation of the FileMapping object.
        """
        return f"FileMapping({self.remote_file_path},\
            {self.local_file_path}, {self.remote_name}, {self.subject_id}, \
            {self.modality})"

    def __repr__(self):
        """
        Return a string representation of the FileMapping object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'file_mappings' table.
        """
        sql_query = """
        CREATE TABLE file_mappings (
            remote_file_path TEXT NOT NULL,
            remote_name TEXT NOT NULL,
            local_file_path TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            modality TEXT NOT NULL,
            PRIMARY KEY (remote_file_path, local_file_path, remote_name, subject_id),
            FOREIGN KEY (remote_file_path, remote_name) REFERENCES remote_files(r_file_path, r_remote_name),
            FOREIGN KEY (local_file_path) REFERENCES phoenix_files(p_file_path),
            FOREIGN KEY (subject_id) REFERENCES subjects(subject_id)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'file_mappings' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS file_mappings;
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'file_mappings' table.
        """
        remote_file_path = db.santize_string(self.remote_file_path)
        local_file_path = db.santize_string(self.local_file_path)
        remote_name = db.santize_string(self.remote_name)
        subject_id = db.santize_string(self.subject_id)
        modality = db.santize_string(self.modality)

        return f"""
            INSERT INTO file_mappings (
                remote_file_path, local_file_path, remote_name,
                subject_id, modality
            ) VALUES (
                '{remote_file_path}', '{local_file_path}', '{remote_name}',
                '{subject_id}', '{modality}'
            ON CONFLICT DO NOTHING;
        """
