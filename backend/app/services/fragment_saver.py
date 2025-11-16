from typing import List, Any, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from bson import ObjectId

from app.models import ParsedFragment


def _clean_preview(obj: Any) -> Any:
    """
    Remove MongoDB _id fields and convert ObjectId to string
    so the preview can be stored in PostgreSQL JSON column.
    """
    if isinstance(obj, dict):
        cleaned: Dict[str, Any] = {}
        for k, v in obj.items():
            if k == "_id":
                continue
            cleaned[k] = _clean_preview(v)
        return cleaned
    elif isinstance(obj, list):
        return [_clean_preview(v) for v in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj


class FragmentSaver:
    @staticmethod
    async def save_json_fragments(
        session: AsyncSession,
        file_id: str,
        json_blocks: List[dict],
    ) -> None:
        """
        json_blocks is a list of blocks, each something like:
        {
            "start_offset": ...,
            "end_offset": ...,
            "record_count": ...,
            "records": [ {...}, {...}, ... ]   # or "data"
        }
        """
        for block in json_blocks:
            records = block.get("records") or block.get("data") or []
            # Take a small preview (first few docs), clean ObjectId/_id
            preview_records = _clean_preview(records[:3])

            fragment = ParsedFragment(
                file_id=file_id,
                fragment_type="json",
                start_offset=block.get("start_offset"),
                end_offset=block.get("end_offset"),
                record_count=len(records) if isinstance(records, list) else block.get("record_count", 0),
                preview_json=preview_records,
            )
            session.add(fragment)

    @staticmethod
    async def save_csv_blocks(
        session: AsyncSession,
        file_id: str,
        csv_blocks: List[dict],
    ) -> None:
        """
        csv_blocks is a list of blocks, each something like:
        {
            "start_offset": ...,
            "end_offset": ...,
            "rows": [ {"col1": "...", "col2": "..."}, ... ]
        }
        """
        for block in csv_blocks:
            rows = block.get("rows") or []
            # Clean preview rows before storing
            preview_rows = _clean_preview(rows[:3])

            fragment = ParsedFragment(
                file_id=file_id,
                fragment_type="csv",
                start_offset=block.get("start_offset"),
                end_offset=block.get("end_offset"),
                record_count=len(rows),
                preview_json=preview_rows,
            )
            session.add(fragment)

    @staticmethod
    async def save_kv_blocks(
        session: AsyncSession,
        file_id: str,
        kv_blocks: List[dict],
    ) -> None:
        """
        kv_blocks example:
        {
            "start_offset": ...,
            "end_offset": ...,
            "pairs": { "key1": "value1", ... }
        }
        """
        for block in kv_blocks:
            pairs = block.get("pairs") or {}
            preview_pairs = _clean_preview(pairs)

            fragment = ParsedFragment(
                file_id=file_id,
                fragment_type="kv",
                start_offset=block.get("start_offset"),
                end_offset=block.get("end_offset"),
                record_count=len(pairs) if isinstance(pairs, dict) else 0,
                preview_json=preview_pairs,
            )
            session.add(fragment)

    @staticmethod
    async def save_html_tables(
        session: AsyncSession,
        file_id: str,
        html_blocks: List[dict],
    ) -> None:
        """
        html_blocks example:
        {
            "start_offset": ...,
            "end_offset": ...,
            "rows": [ {"col1": "...", "col2": "..."}, ... ]
        }
        """
        for block in html_blocks:
            rows = block.get("rows") or []
            preview_rows = _clean_preview(rows[:3])

            fragment = ParsedFragment(
                file_id=file_id,
                fragment_type="html",
                start_offset=block.get("start_offset"),
                end_offset=block.get("end_offset"),
                record_count=len(rows),
                preview_json=preview_rows,
            )
            session.add(fragment)
