import re
import json
from typing import List, Dict, Any
from bs4 import BeautifulSoup


class FragmentExtractor:
    @staticmethod
    def extract_json_blocks(text: str) -> List[str]:
        candidates = re.findall(r"\{.*?\}", text, flags=re.DOTALL)
        valid = []
        for c in candidates:
            try:
                parsed = json.loads(c)
                if isinstance(parsed, dict):
                    valid.append(c)
            except Exception:
                continue
        return valid

    @staticmethod
    def extract_csv_blocks(text: str) -> List[str]:
        lines = text.splitlines()
        blocks = []
        current = []
        for line in lines:
            if "," in line and ":" not in line:
                current.append(line)
            else:
                if len(current) > 1:
                    blocks.append("\n".join(current))
                current = []
        if len(current) > 1:
            blocks.append("\n".join(current))
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
