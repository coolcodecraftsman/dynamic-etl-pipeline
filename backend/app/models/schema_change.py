import uuid
from sqlalchemy import Column, String, ForeignKey, JSON
from app.core.database import Base


class SchemaChange(Base):
    __tablename__ = "schema_changes"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_id = Column(String, ForeignKey("sources.source_id"), index=True)

    from_version = Column(String)
    to_version = Column(String)

    diff_json = Column(JSON)  # added_fields, removed, renamed, type_changed...
