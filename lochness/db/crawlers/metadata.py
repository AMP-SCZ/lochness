"""
Imports metadata information into DB.
"""

from typing import Any, Dict
import logging

import pandas as pd

from lochness import db
from lochness.db.models.subject import Subject
from lochness.db.models.study import Study

logger = logging.getLogger("lochness.crawlers.metadata")


def import_metadata_df(
    lochness_config: Dict[str, Any], metadata_df: pd.DataFrame, study_id: str
) -> None:
    """
    Import subject metadata from a DataFrame into the database.

    Args:
        metadata_df (pd.DataFrame): The DataFrame containing the columns:
            'Subject ID', 'Active', 'Consent', '...'.
            Note.: This DataFrame is generally obtained from RPMS or REDCap modules.
        study_id (str): The study ID.
    """
    logger.info(f"Importing metadata for study {study_id}")
    queries = []

    study = Study(study_id=study_id)
    insert_study_sql = study.to_sql()
    queries.append(insert_study_sql)

    logger.debug(f"Found {metadata_df.shape[0]} subjects for study {study_id}")
    for _, row in metadata_df.iterrows():
        optional_notes = {}
        for column in metadata_df.columns:
            if column not in ["Subject ID", "Active", "Consent"]:
                optional_notes[column] = row[column]

        consent_date = pd.to_datetime(row["Consent"]).to_pydatetime()
        is_active = row["Active"] == 1
        subject = Subject(
            study_id=study_id,
            subject_id=row["Subject ID"],
            is_active=is_active,
            consent_date=consent_date,
            optional_notes=optional_notes,
        )

        subject_sql = subject.to_sql()
        queries.append(subject_sql)

    db.execute_queries(lochness_config=lochness_config, queries=queries, show_commands=False)
    logger.info(f"Successfully imported metadata for study {study_id}")
