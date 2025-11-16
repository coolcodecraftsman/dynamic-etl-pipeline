import uuid
from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.sql import func
from app.models import Base


class UploadedFile(Base):
    __tablename__ = "uploaded_files"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_id = Column(String, index=True, nullable=True)
    filename = Column(String, nullable=False)
    content_type = Column(String, nullable=True)
    size_bytes = Column(Integer, nullable=True)
    content_hash = Column(String, index=True, nullable=True)
    raw_text_excerpt = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
