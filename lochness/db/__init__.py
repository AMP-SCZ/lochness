"""
Helper functions for interacting with a PostgreSQL database.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Callable, Dict, Literal, Optional, Any
import hashlib

import pandas as pd
import psycopg2
import sqlalchemy

logger = logging.getLogger(__name__)


def compute_hash(file_path: Path, hash_type: str = "md5") -> str:
    """
    Compute the hash digest of a file.

    Args:
        file_path (Path): The path to the file.
        hash_type (str, optional): The type of hash algorithm to use. Defaults to 'md5'.

    Returns:
        str: The computed hash digest of the file.
    """
    with open(file_path, "rb") as file:
        file_hash = hashlib.file_digest(file, hash_type)
        hash_str = file_hash.hexdigest()

    return hash_str


def handle_null(query: str) -> str:
    """
    Replaces all occurrences of the string 'NULL' with the SQL NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'NULL' replaced with NULL.
    """
    query = query.replace("'NULL'", "NULL")

    return query


def handle_nan(query: str) -> str:
    """
    Replaces all occurrences of the string 'nan' with the SQL NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'nan' replaced with NULL.
    """
    query = query.replace("'nan'", "NULL")

    return query


def santize_string(string: str) -> str:
    """
    Sanitizes a string by escaping single quotes.

    Args:
        string (str): The string to sanitize.

    Returns:
        str: The sanitized string.
    """
    return string.replace("'", "''")


def sanitize_json(json_dict: dict) -> str:
    """
    Sanitizes a JSON object by replacing single quotes with double quotes.

    Args:
        json_dict (dict): The JSON object to sanitize.

    Returns:
        str: The sanitized JSON object.
    """
    for key, value in json_dict.items():
        if isinstance(value, str):
            json_dict[key] = santize_string(value)

    json_str = json.dumps(json_dict, default=str)

    # Replace NaN with NULL
    json_str = json_str.replace("NaN", "null")

    return json_str


def on_failure():
    """
    Exits the program with exit code 1.
    """
    sys.exit(1)


def get_db_credentials(lochness_config: Dict[str, Any]) -> Dict[str, str]:
    """
    Retrieves the database credentials from the configuration file.

    Args:
        lochness_config: Dict[str, Any]: The Lochness configuration dictionary.
        db (str, optional): The section of the configuration file to use.
            Defaults to "postgresql".

    Returns:
        Dict[str, str]: A dictionary containing the database credentials.
    """
    credentials = lochness_config["database"]

    return credentials


def execute_queries(
    lochness_config: Dict[str, Any],
    queries: list,
    show_commands=True,
    silent=False,
    on_failure: Optional[Callable] = on_failure,
) -> list:
    """
    Executes a list of SQL queries on a PostgreSQL database.

    Args:
        lochness_config: Dict[str, Any]: The Lochness configuration dictionary.
        queries (list): A list of SQL queries to execute.
        show_commands (bool, optional): Whether to display the executed SQL queries.
            Defaults to True.
        show_progress (bool, optional): Whether to display a progress bar. Defaults to False.
        silent (bool, optional): Whether to suppress output. Defaults to False.
        db (str, optional): The section of the configuration file to use.
            Defaults to "postgresql".
        backup (bool, optional): Whether to sace all executed queries to a file.

    Returns:
        list: A list of tuples containing the results of the executed queries.
    """
    command = None
    output = []

    try:
        credentials = get_db_credentials(lochness_config=lochness_config)
        conn: psycopg2.extensions.connection = psycopg2.connect(**credentials)  # type: ignore
        cur = conn.cursor()

        def execute_query(query: str):
            if show_commands:
                logger.debug("Executing query:")
                logger.debug(f"[bold blue]{query}", extra={"markup": True})
            cur.execute(query)
            try:
                output.append(cur.fetchall())
            except psycopg2.ProgrammingError:
                pass
        for command in queries:
            execute_query(command)

        cur.close()

        conn.commit()

        if not silent:
            logger.debug(
                f"[grey]Executed {len(queries)} SQL query(ies).", extra={"markup": True}
            )
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error("[bold red]Error executing queries.", extra={"markup": True})
        if command is not None:
            logger.error(f"[red]For query: {command}", extra={"markup": True})
        logger.error(e)
        if on_failure is not None:
            on_failure()
        else:
            raise e
    finally:
        if conn is not None:
            conn.close()

    return output


def get_db_connection(lochness_config: Dict[str, Any]) -> sqlalchemy.engine.base.Engine:
    """
    Establishes a connection to the PostgreSQL database using the provided configuration file.

    Args:
        lochness_config (Dict[str, Any]): The Lochness configuration dictionary.

    Returns:
        sqlalchemy.engine.base.Engine: The database connection engine.
    """
    credentials = get_db_credentials(lochness_config=lochness_config)
    engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://"
        + credentials["user"]
        + ":"
        + credentials["password"]
        + "@"
        + credentials["host"]
        + ":"
        + credentials["port"]
        + "/"
        + credentials["database"]
    )

    return engine


def execute_sql(
    lochness_config: Dict[str, Any], query: str, debug: bool = False
) -> pd.DataFrame:
    """
    Executes a SQL query on a PostgreSQL database and returns the result as a pandas DataFrame.

    Args:
        lochness_config: Dict[str, Any]: The Lochness configuration dictionary.
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the result of the SQL query.
    """
    engine = get_db_connection(lochness_config=lochness_config)

    if debug:
        logger.debug(f"Executing query: {query}")

    df = pd.read_sql(query, engine)

    engine.dispose()

    return df


def fetch_record(lochness_config: Path, query: str) -> Optional[str]:
    """
    Fetches a single record from the database using the provided SQL query.

    Args:
        config_file_path (str): The path to the database configuration file.
        query (str): The SQL query to execute.

    Returns:
        Optional[str]: The value of the first column of the first row of the result set,
        or None if the result set is empty.
    """
    df = execute_sql(lochness_config=lochness_config, query=query)

    # Check if there is a row
    if df.shape[0] == 0:
        return None

    value = df.iloc[0, 0]

    return str(value)


def df_to_table(
    lochness_config: Dict[str, Any],
    df: pd.DataFrame,
    table_name: str,
    if_exists: Literal["fail", "replace", "append"] = "replace",
) -> None:
    """
    Writes a pandas DataFrame to a table in a PostgreSQL database.

    Args:
        lochness_config (Dict[str, Any]): The Lochness configuration dictionary.
        df (pd.DataFrame): The DataFrame to write to the database.
        table_name (str): The name of the table to write to.
        if_exists (Literal["fail", "replace", "append"], optional): What to do
            if the table already exists.
    """

    engine = get_db_connection(lochness_config=lochness_config)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    engine.dispose()
