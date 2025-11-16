from fastapi import APIRouter, UploadFile, File, Form, Depends
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.services.file_service import FileService
from app.services.fragment_extractor import FragmentExtractor
from app.services.fragment_saver import FragmentSaver

router = APIRouter(tags=["upload"])


@router.post("/upload")
async def upload_file(
    source_id: Optional[str] = Form(None),
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
    session: AsyncSession = Depends(get_db),
):
    file_bytes = await file.read()
    content_hash = await FileService.compute_hash(file_bytes)
    extracted_text = await FileService.extract_text(file, file_bytes)

    uploaded_file = await FileService.save_file_record(
        session=session,
        source_id=source_id,
        file=file,
        content_hash=content_hash,
        text_excerpt=extracted_text,
        size_bytes=len(file_bytes),
    )

    json_blocks = FragmentExtractor.extract_json_blocks(extracted_text)
    csv_blocks = FragmentExtractor.extract_csv_blocks(extracted_text)
    kv_blocks = FragmentExtractor.extract_kv_blocks(extracted_text)
    html_tables = FragmentExtractor.extract_html_tables(extracted_text)
    text_block = FragmentExtractor.extract_text_block(extracted_text)

    await FragmentSaver.save_json_fragments(session, uploaded_file.id, json_blocks)
    await FragmentSaver.save_csv_blocks(session, uploaded_file.id, csv_blocks)
    await FragmentSaver.save_html_tables(session, uploaded_file.id, html_tables)
    await FragmentSaver.save_kv_blocks(session, uploaded_file.id, kv_blocks)
    if text_block:
        await FragmentSaver.save_text_block(session, uploaded_file.id, text_block)

    return {
        "status": "ok",
        "source_id": source_id,
        "file_id": uploaded_file.id,
        "filename": file.filename,
        "content_hash": content_hash,
        "raw_text_length": len(extracted_text),
        "fragments": {
            "json_blocks": len(json_blocks),
            "csv_blocks": len(csv_blocks),
            "kv_blocks": len(kv_blocks),
            "html_tables": len(html_tables),
            "text_block": bool(text_block),
        },
    }
