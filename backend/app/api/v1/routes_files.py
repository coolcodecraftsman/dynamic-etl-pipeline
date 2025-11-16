from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.core.database import get_db
from app.models.uploaded_file import UploadedFile
from app.models.parsed_fragment import ParsedFragment

router = APIRouter(tags=["files"])


@router.get("/files")
async def list_files(
    session: AsyncSession = Depends(get_db),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    stmt = (
        select(UploadedFile)
        .order_by(UploadedFile.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(stmt)
    rows: List[UploadedFile] = result.scalars().all()

    return [
        {
            "id": r.id,
            "source_id": r.source_id,
            "filename": r.filename,
            "content_type": r.content_type,
            "size_bytes": r.size_bytes,
            "created_at": r.created_at,
        }
        for r in rows
    ]


@router.get("/files/{file_id}/fragments")
async def list_fragments_for_file(
    file_id: str,
    session: AsyncSession = Depends(get_db),
    fragment_type: Optional[str] = Query(None),
):
    stmt = select(ParsedFragment).where(ParsedFragment.file_id == file_id)
    if fragment_type:
        stmt = stmt.where(ParsedFragment.fragment_type == fragment_type)
    stmt = stmt.order_by(ParsedFragment.fragment_type, ParsedFragment.id)

    result = await session.execute(stmt)
    rows: List[ParsedFragment] = result.scalars().all()

    if not rows:
        stmt_file = select(UploadedFile).where(UploadedFile.id == file_id)
        result_file = await session.execute(stmt_file)
        file_row = result_file.scalars().first()
        if not file_row:
            raise HTTPException(status_code=404, detail="File not found")

    return [
        {
            "id": r.id,
            "file_id": r.file_id,
            "fragment_type": r.fragment_type,
            "start_offset": r.start_offset,
            "end_offset": r.end_offset,
            "record_count": r.record_count,
            "preview_json": r.preview_json,
        }
        for r in rows
    ]


@router.get("/sources")
async def list_sources(
    session: AsyncSession = Depends(get_db),
):
    stmt = (
        select(
            UploadedFile.source_id,
            func.count(UploadedFile.id),
        )
        .where(UploadedFile.source_id.is_not(None))
        .group_by(UploadedFile.source_id)
        .order_by(UploadedFile.source_id)
    )
    result = await session.execute(stmt)
    rows = result.all()

    return [
        {
            "source_id": r[0],
            "file_count": r[1],
        }
        for r in rows
    ]
