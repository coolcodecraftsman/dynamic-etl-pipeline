from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from app.core.database import get_db
from app.models.schema_version import SchemaVersion
from app.services.schema_inference import SchemaInferenceService
from app.services.schema_diff import SchemaDiffService

router = APIRouter(tags=["schema"])


@router.post("/schema/infer/{source_id}")
async def infer_schema_for_source(
    source_id: str,
    session: AsyncSession = Depends(get_db),
):
    try:
        schema_row = await SchemaInferenceService.infer_for_source(session, source_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "source_id": schema_row.source_id,
        "version": schema_row.version,
        "schema": schema_row.schema_json,
        "created_at": schema_row.created_at,
    }


@router.get("/schema/{source_id}/latest")
async def get_latest_schema(
    source_id: str,
    session: AsyncSession = Depends(get_db),
):
    stmt = (
        select(SchemaVersion)
        .where(SchemaVersion.source_id == source_id)
        .order_by(desc(SchemaVersion.version))
    )
    result = await session.execute(stmt)
    row = result.scalars().first()
    if not row:
        raise HTTPException(status_code=404, detail="No schema for this source_id")
    return {
        "source_id": row.source_id,
        "version": row.version,
        "schema": row.schema_json,
        "created_at": row.created_at,
    }


@router.get("/schema/{source_id}/versions")
async def list_schema_versions(
    source_id: str,
    session: AsyncSession = Depends(get_db),
):
    stmt = (
        select(SchemaVersion)
        .where(SchemaVersion.source_id == source_id)
        .order_by(SchemaVersion.version)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return [
        {
            "source_id": r.source_id,
            "version": r.version,
            "created_at": r.created_at,
        }
        for r in rows
    ]


@router.get("/schema/compare/{source_id}")
async def compare_schema_versions(
    source_id: str,
    v1: int,
    v2: int,
    session: AsyncSession = Depends(get_db),
):
    stmt_v1 = (
        select(SchemaVersion)
        .where(
            SchemaVersion.source_id == source_id,
            SchemaVersion.version == v1,
        )
        .limit(1)
    )
    stmt_v2 = (
        select(SchemaVersion)
        .where(
            SchemaVersion.source_id == source_id,
            SchemaVersion.version == v2,
        )
        .limit(1)
    )

    result_v1 = await session.execute(stmt_v1)
    row_v1 = result_v1.scalars().first()
    if not row_v1:
        raise HTTPException(status_code=404, detail="Version v1 not found")

    result_v2 = await session.execute(stmt_v2)
    row_v2 = result_v2.scalars().first()
    if not row_v2:
        raise HTTPException(status_code=404, detail="Version v2 not found")

    diff = SchemaDiffService.diff(row_v1.schema_json, row_v2.schema_json)

    return {
        "source_id": source_id,
        "v1": v1,
        "v2": v2,
        "diff": diff,
    }
