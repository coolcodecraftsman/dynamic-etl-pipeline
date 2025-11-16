import hashlib
import mimetypes
import uuid
from typing import Optional
from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.uploaded_file import UploadedFile
import pdfplumber


class FileService:
    @staticmethod
    async def compute_hash(file_bytes: bytes) -> str:
        return hashlib.sha256(file_bytes).hexdigest()

    @staticmethod
    async def extract_text(file: UploadFile, file_bytes: bytes) -> str:
        mime = file.content_type or mimetypes.guess_type(file.filename)[0]
        if mime is None:
            return file_bytes.decode("utf-8", errors="ignore")

        mime = mime.lower()

        if "text/plain" in mime:
            return file_bytes.decode("utf-8", errors="ignore")

        if file.filename.lower().endswith(".md"):
            return file_bytes.decode("utf-8", errors="ignore")

        if "pdf" in mime:
            try:
                with pdfplumber.open(file.file) as pdf:
                    pages = [page.extract_text() or "" for page in pdf.pages]
                    return "\n".join(pages)
            except Exception:
                return ""

        return file_bytes.decode("utf-8", errors="ignore")

    @staticmethod
    async def save_file_record(
        session: AsyncSession,
        source_id: Optional[str],
        file: UploadFile,
        content_hash: str,
        text_excerpt: str,
        size_bytes: int,
    ) -> UploadedFile:
        if source_id:
            stmt = select(UploadedFile).where(
                UploadedFile.source_id == source_id,
                UploadedFile.content_hash == content_hash,
            )
            result = await session.execute(stmt)
            existing = result.scalars().first()
            if existing:
                return existing
        else:
            stmt = select(UploadedFile).where(UploadedFile.content_hash == content_hash)
            result = await session.execute(stmt)
            existing = result.scalars().first()
            if existing:
                return existing

        filename = file.filename
        if not filename:
            filename = source_id or str(uuid.uuid4())

        uploaded = UploadedFile(
            source_id=source_id,
            filename=filename,
            content_type=file.content_type,
            size_bytes=size_bytes,
            content_hash=content_hash,
            raw_text_excerpt=text_excerpt[:1000],
        )
        session.add(uploaded)
        await session.commit()
        await session.refresh(uploaded)
        return uploaded
