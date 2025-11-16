import json
from typing import Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from app.models.uploaded_file import UploadedFile
from app.models.parsed_fragment import ParsedFragment
from app.models.schema_version import SchemaVersion
from app.core.database import get_mongo_db


class SchemaInferenceService:
    @staticmethod
    def infer_type(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, (int, float)):
            return "number"
        if isinstance(value, dict):
            return "object"
        if isinstance(value, list):
            return "array"
        return "string"

    @staticmethod
    def merge_field_types(existing: Dict[str, Any], new_doc: Dict[str, Any]) -> None:
        for key, value in new_doc.items():
            t = SchemaInferenceService.infer_type(value)
            if key not in existing:
                existing[key] = {"types": set([t])}
            else:
                existing[key]["types"].add(t)

    @staticmethod
    def finalize_schema(fields: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for key, meta in fields.items():
            types = sorted(list(meta["types"]))
            result[key] = {"types": types}
        return result

    @staticmethod
    async def infer_for_source(session: AsyncSession, source_id: str) -> SchemaVersion:
        mongo = get_mongo_db()

        stmt_files = select(UploadedFile.id).where(UploadedFile.source_id == source_id)
        result_files = await session.execute(stmt_files)
        file_ids = [row[0] for row in result_files.all()]
        if not file_ids:
            raise ValueError("No files for this source_id")

        stmt_frags = select(ParsedFragment).where(
            ParsedFragment.file_id.in_(file_ids)
        )
        result_frags = await session.execute(stmt_frags)
        frags: List[ParsedFragment] = result_frags.scalars().all()

        fields: Dict[str, Any] = {}

        for frag in frags:
            if frag.fragment_type == "json":
                cursor = mongo.json_fragments.find({"fragment_id": frag.id}).limit(50)
                async for doc in cursor:
                    doc.pop("_id", None)
                    SchemaInferenceService.merge_field_types(fields, doc)
            elif frag.fragment_type == "csv":
                cursor = mongo.csv_fragments.find({"fragment_id": frag.id}).limit(50)
                async for doc in cursor:
                    doc.pop("_id", None)
                    SchemaInferenceService.merge_field_types(fields, doc)
            elif frag.fragment_type == "html":
                cursor = mongo.html_tables.find({"fragment_id": frag.id}).limit(50)
                async for doc in cursor:
                    doc.pop("_id", None)
                    SchemaInferenceService.merge_field_types(fields, doc)
            elif frag.fragment_type == "kv":
                cursor = mongo.kv_fragments.find({"fragment_id": frag.id}).limit(50)
                async for doc in cursor:
                    doc.pop("_id", None)
                    SchemaInferenceService.merge_field_types(fields, doc)

        schema_dict = SchemaInferenceService.finalize_schema(fields)
        schema_json = json.dumps(schema_dict)

        stmt_latest = (
            select(SchemaVersion)
            .where(SchemaVersion.source_id == source_id)
            .order_by(desc(SchemaVersion.version))
        )
        result_latest = await session.execute(stmt_latest)
        latest = result_latest.scalars().first()
        next_version = 1 if latest is None else latest.version + 1

        schema_row = SchemaVersion(
            source_id=source_id,
            version=next_version,
            schema_json=schema_json,
        )
        session.add(schema_row)
        await session.commit()
        await session.refresh(schema_row)
        return schema_row
