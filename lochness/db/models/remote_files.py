from datetime import datetime
from typing import Dict, List
from sqlalchemy import create_engine, Column, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session as SessionType
from lochness import db


# Define the database connection (replace with your actual database URI)
DATABASE_URI = 'postgresql://username:password@localhost:5432/mydatabase'

# Create the SQLAlchemy engine and session
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

Base = declarative_base()

class RemoteFile(Base):
    __tablename__ = 'remote_files'

    file_path = Column(String, primary_key=True)
    remote_name = Column(String, primary_key=True)
    hash_val = Column(String, nullable=False)
    last_checked = Column(DateTime, nullable=False)
    remote_metadata = Column(JSON, nullable=True)

    def __init__(
        self,
        file_path: str,
        remote_name: str,
        hash_val: str,
        last_checked: datetime,
        remote_metadata: Dict[str, str],
    ):
        self.file_path = file_path
        self.remote_name = remote_name
        self.hash_val = hash_val
        self.last_checked = last_checked
        self.remote_metadata = remote_metadata

    def __str__(self):
        return f"RemoteFile({self.file_path}, {self.remote_name}, {self.last_checked})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def find_matches_by_hash(session: SessionType, hash_val: str) -> List['RemoteFile']:
        return session.query(RemoteFile).filter_by(hash_val=hash_val).all()

    def save(self, session: SessionType):
        session.add(self)
        session.commit()

    @staticmethod
    def drop_table(engine):
        Base.metadata.drop_all(engine, [RemoteFile.__table__])

# Create the table is equivalent to the previous init_table_query method
Base.metadata.create_all(engine)

# Example usage
if __name__ == "__main__":
    # Create a session
    session = Session()

    # Example to add a RemoteFile
    remote_file = RemoteFile(
        file_path=db.santize_string('/path/to/file'),
        remote_name=db.santize_string('remote_system'),
        hash_val=db.santize_string(('abcdef123456'),
        last_checked=datetime.now(),
        remote_metadata={'key': 'value'}
    )
    remote_file.save(session)

    # Find matches by hash
    hash_val = 'abcdef123456'
    matches = RemoteFile.find_matches_by_hash(session, hash_val)
    for match in matches:
        print(match)

    # Drop the table -> this line is equivalent to previous drop_table method
    RemoteFile.drop_table(engine)

    # Close the session
    session.close()
