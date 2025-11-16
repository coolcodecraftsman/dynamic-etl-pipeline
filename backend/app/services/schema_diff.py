import json
from typing import Dict, Any


class SchemaDiffService:
    @staticmethod
    def _parse_schema(schema_json: str) -> Dict[str, Any]:
        try:
            return json.loads(schema_json)
        except Exception:
            return {}

    @staticmethod
    def diff(schema_json_old: str, schema_json_new: str) -> Dict[str, Any]:
        old = SchemaDiffService._parse_schema(schema_json_old)
        new = SchemaDiffService._parse_schema(schema_json_new)

        old_fields = set(old.keys())
        new_fields = set(new.keys())

        added_fields = sorted(list(new_fields - old_fields))
        removed_fields = sorted(list(old_fields - new_fields))
        common_fields = old_fields & new_fields

        changed_fields = {}

        for field in common_fields:
            old_types = sorted(old.get(field, {}).get("types", []))
            new_types = sorted(new.get(field, {}).get("types", []))
            if old_types != new_types:
                changed_fields[field] = {
                    "old_types": old_types,
                    "new_types": new_types,
                }

        return {
            "added_fields": added_fields,
            "removed_fields": removed_fields,
            "changed_fields": changed_fields,
        }
