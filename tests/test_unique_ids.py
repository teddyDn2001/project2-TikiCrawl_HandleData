from pathlib import Path

import json


OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def test_unique_ids():
    all_ids: list[int | str] = []

    for path in OUTPUT_DIR.glob("products_*.json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            if not isinstance(item, dict):
                continue
            if "id" in item:
                all_ids.append(item["id"])

    assert all_ids, "Không tìm thấy ID nào trong output, hãy chạy pipeline trước khi test."
    assert len(all_ids) == len(set(all_ids)), "Duplicate product IDs found in output."

