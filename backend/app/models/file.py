import uuid
from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.sql import func
from app.core.database import Base


class File(Base):
    __tablename__ = "files"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_id = Column(String, ForeignKey("sources.source_id"), index=True)

    file_name = Column(String)
    mime_type = Column(String)
    content_hash = Column(String, index=True)
    raw_text_excerpt = Column(String)

    uploaded_at = Column(DateTime(timezone=True), server_default=func.now())
