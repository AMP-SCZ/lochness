"""
Contains helper functions to log information to DB.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from lochness import db
from lochness.db.models.file_mappings import FileMapping
from lochness.db.models.files_audit_log import AuditLog
from lochness.db.models.phoenix_files import PhoenixFiles
from lochness.db.models.remote_files import RemoteFile

logger = logging.getLogger("lochness.db.log")


def log_download(
    lochness_config: Dict[str, Any],
    remote_file_path: Path,
    remote_name: str,
    remote_hash: Optional[str],
    remote_metadata: Dict[str, Any],
    local_file_path: Path,
    subject_id: str,
    study_id: str,
    modality: str,
) -> None:
    """
    Logs a download operation to the audit_log table.

    Adds entries in:
    - audit_log
    - file_mappings
    - phoenix_files
    - remote_files
    """

    logger.info(f"Logging download of {remote_file_path} to {local_file_path}")

    # Log download to audit_log
    audit_log_entry = AuditLog(
        source_file=remote_file_path,
        destination_file=local_file_path,
        action="download",
        system=remote_name,
        metadata={},
        timestamp=datetime.now(),
    )

    # Log to remote_files
    remote_file = RemoteFile(
        file_path=remote_file_path,
        remote_name=remote_name,
        hash_val=remote_hash,
        last_checked=datetime.now(),
        remote_metadata=remote_metadata,
    )

    # log to phoenix_files
    phoenix_file = PhoenixFiles(file_path=local_file_path, with_hash=True)

    # log to file_mappings
    file_mapping = FileMapping(
        remote_file_path=remote_file_path,
        local_file_path=local_file_path,
        remote_name=remote_name,
        subject_id=subject_id,
        study_id=study_id,
        modality=modality,
    )

    queries: List[str] = []
    queries.append(audit_log_entry.to_sql())
    queries.append(remote_file.to_sql())
    queries.append(phoenix_file.to_sql())
    queries.append(file_mapping.to_sql())

    db.execute_queries(lochness_config=lochness_config, queries=queries)
