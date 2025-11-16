import uuid
from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.sql import func
from app.models import Base


class SchemaVersion(Base):
    __tablename__ = "schema_versions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_id = Column(String, index=True, nullable=False)
    version = Column(Integer, nullable=False)
    schema_json = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
