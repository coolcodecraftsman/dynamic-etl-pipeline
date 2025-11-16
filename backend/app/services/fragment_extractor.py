import re
import json
import csv
from io import StringIO
from typing import List, Dict, Any
from bs4 import BeautifulSoup


class FragmentExtractor:
    @staticmethod
    def extract_json_blocks(text: str) -> List[Dict[str, Any]]:
        candidates = re.findall(r"\{.*?\}", text, flags=re.DOTALL)
        valid = []
        for c in candidates:
            try:
                parsed = json.loads(c)
                if isinstance(parsed, dict):
                    valid.append(parsed)
            except Exception:
                continue
        return valid

    @staticmethod
    def extract_csv_blocks(text: str) -> List[Dict[str, Any]]:
        lines = text.splitlines()
        blocks = []
        current_block_lines = []
        for line in lines:
            # Simple heuristic: if a line contains a comma and not a colon,
            # it's likely part of a CSV block.
            if "," in line and ":" not in line:
                current_block_lines.append(line)
            else:
                if len(current_block_lines) > 1:
                    # Process the collected CSV block
                    block_text = "\n".join(current_block_lines)
                    try:
                        reader = csv.DictReader(StringIO(block_text))
                        rows = [row for row in reader]
                        if rows:
                            blocks.append({"rows": rows})
                    except Exception:
                        pass  # Or log the error if needed
                current_block_lines = []

        # Process the last collected block if any
        if len(current_block_lines) > 1:
            block_text = "\n".join(current_block_lines)
            try:
                reader = csv.DictReader(StringIO(block_text))
                rows = [row for row in reader]
                if rows:
                    blocks.append({"rows": rows})
            except Exception:
                pass
        return blocks

    @staticmethod
    def extract_kv_blocks(text: str) -> List[Dict[str, str]]:
        pattern = r"([A-Za-z0-9_ ]+):\s*([^\n]+)"
        matches = re.findall(pattern, text)
        if not matches:
            return []
        kv = {k.strip(): v.strip() for k, v in matches}
        return [kv]

    @staticmethod
    def extract_html_tables(text: str) -> List[List[Dict[str, Any]]]:
        soup = BeautifulSoup(text, "html.parser")
        tables = soup.find_all("table")
        result = []
        for table in tables:
            rows_raw = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
                if cells:
                    rows_raw.append(cells)
            if len(rows_raw) > 1:
                header = rows_raw[0]
                rows = [dict(zip(header, row)) for row in rows_raw[1:]]
                result.append(rows)
        return result

    @staticmethod
    def extract_text_block(text: str) -> str:
        return text.strip()
