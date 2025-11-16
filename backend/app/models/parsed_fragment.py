import uuid
from sqlalchemy import Column, String, Integer, ForeignKey, Text
from app.models import Base


class ParsedFragment(Base):
    __tablename__ = "parsed_fragments"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    file_id = Column(String, ForeignKey("uploaded_files.id"), index=True, nullable=False)
    fragment_type = Column(String, index=True, nullable=False)
    start_offset = Column(Integer, nullable=True)
    end_offset = Column(Integer, nullable=True)
    record_count = Column(Integer, nullable=True)
    preview_json = Column(Text, nullable=True)
