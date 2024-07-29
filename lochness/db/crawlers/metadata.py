"""
Imports metadata information into DB.
"""

from typing import Any, Dict

import pandas as pd

from lochness import db
from lochness.db.models.subject import Subject
from lochness.db.models.study import Study


def import_metadata_df(
    lochness_config: Dict[str, Any], metadata_df: pd.DataFrame, study_id: str
) -> None:
    """
    Import subject metadata from a DataFrame into the database.

    Args:
        metadata_df (pd.DataFrame): The DataFrame containing the columns:
            'Subject ID', 'Active', 'Consent', '...'.
        study_id (str): The study ID.
    """
    queries = []

    study = Study(study_id=study_id)
    insert_study_sql = study.to_sql()
    queries.append(insert_study_sql)

    for _, row in metadata_df.iterrows():
        optional_notes = {}
        for column in metadata_df.columns:
            if column not in ["Subject ID", "Active", "Consent"]:
                optional_notes[column] = row[column]

        subject = Subject(
            study_id=study_id,
            subject_id=row["Subject ID"],
            is_active=row["Active"],
            consent_date=row["Consent"],
            optional_notes=optional_notes,
        )

        subject_sql = subject.to_sql()
        queries.append(subject_sql)

    db.execute_queries(lochness_config=lochness_config, queries=queries)
