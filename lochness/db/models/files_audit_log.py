#!/usr/bin/env python
"""
AuditLog Model
"""
from typing import Dict

from lochness import db


class AuditLog:
    """
    Represents an audit log entry.

    Attributes:
        source_file (str): The source file.
        destination_file (str): The destination file.
        system (str): The system the action was taken on.
        action (str): The action taken.
        metadata (Dict[str, str]): Metadata about the action.
        timestamp (datetime): The time the action was taken.
    """

    def __init__(
        self,
        source_file: str,
        destination_file: str,
        system: str,
        action: str,
        metadata: Dict[str, str],
        timestamp: str,
    ):
        """
        Initialize an AuditLog object.

        Args:
            source_file (str): The source file.
            destination_file (str): The destination file.
            system (str): The system the action was taken on.  e.g. 'local', 'dropbox', etc.
            action (str): The action taken.  e.g. 'move', 'delete', etc.
            metadata (Dict[str, str]): Metadata about the action.
            timestamp (str): The time the action was taken.
        """

        self.source_file = source_file
        self.destination_file = destination_file
        self.system = system
        self.action = action
        self.metadata = metadata
        self.timestamp = timestamp

    def __str__(self):
        """
        Return a string representation of the AuditLog object.
        """
        return f"""
AuditLog(
    {self.source_file},
    {self.destination_file},
    {self.system},
    {self.action},
    {self.metadata},
    {self.timestamp}
)
"""

    def __repr__(self):
        """
        Return a string representation of the AuditLog object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'audit_log' table.
        """
        sql_query = """
        CREATE TABLE IF audit_log (
            source_file TEXT NOT NULL,
            destination_file TEXT,
            system TEXT NOT NULL,
            action TEXT NOT NULL,
            metadata JSONB,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'audit_log' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS audit_log CASCADE;
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'audit_log' table.
        """
        source_file = db.santize_string(self.source_file)
        destination_file = db.santize_string(self.destination_file)
        system = db.santize_string(self.system)
        action = db.santize_string(self.action)
        metadata = db.santize_string(self.metadata)
        timestamp = db.santize_string(self.timestamp)

        sql_query = f"""
        INSERT INTO audit_log (
            source_file, destination_file, system, 
            action, metadata, timestamp
        ) VALUES (
            '{source_file}', '{destination_file}', '{system}',
            '{action}', '{metadata}', '{timestamp}'
        );
        """

        return sql_query
