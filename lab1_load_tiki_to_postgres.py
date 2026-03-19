#!/usr/bin/env python3
"""
Lab 1 (DoD):
Sử dụng Python đọc toàn bộ dữ liệu Tiki từ Project 02 (các file products_*.json)
và ghi vào PostgreSQL.

Input kỳ vọng: list[dict] trong mỗi file JSON với keys:
  - id (int)
  - name (str)
  - url_key (str)
  - price (int/float)
  - description (str)
  - images_url (list[str])
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv

try:
    import orjson as _json_impl

    def json_loads(b: bytes):
        return _json_impl.loads(b)

except ImportError:
    try:
        import ujson as _json_impl  # type: ignore[no-redef]

        def json_loads(b: bytes):
            return _json_impl.loads(b.decode("utf-8", errors="ignore"))

    except ImportError:
        import json as _json_impl  # type: ignore[no-redef]

        def json_loads(b: bytes):
            return _json_impl.loads(b.decode("utf-8", errors="ignore"))


import psycopg2
import psycopg2.extras


DEFAULT_INPUT_DIRS = ["output", "data"]


def iter_product_files(input_dir: Path, pattern: str) -> list[Path]:
    files = sorted(input_dir.glob(pattern))
    return [p for p in files if p.is_file()]


def read_products_from_file(path: Path) -> list[dict]:
    raw = path.read_bytes()
    data = json_loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"File {path} không phải JSON array.")
    rows: list[dict] = []
    for item in data:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def batched(iterable: Iterable[dict], batch_size: int) -> Iterable[list[dict]]:
    buf: list[dict] = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= batch_size:
            yield buf
            buf = []
    if buf:
        yield buf


def connect_postgres():
    """
    DSN ưu tiên:
    - Env var DATABASE_URL (ví dụ: postgresql://user:pass@localhost:5432/db)
    - Hoặc PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE
    """
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        dbname=os.getenv("PGDATABASE", "postgres"),
    )


def ensure_table(conn, ddl_path: Path) -> None:
    sql = ddl_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def upsert_products(conn, products: list[dict]) -> int:
    """
    Upsert theo primary key id để chạy lại nhiều lần không bị duplicate.
    """
    values = []
    for p in products:
        pid = p.get("id")
        if pid is None:
            continue
        values.append(
            (
                int(pid),
                p.get("name"),
                p.get("url_key"),
                p.get("price"),
                p.get("description"),
                psycopg2.extras.Json(p.get("images_url") or []),
            )
        )

    if not values:
        return 0

    sql = """
    INSERT INTO tiki_product (id, name, url_key, price, description, images_url)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      url_key = EXCLUDED.url_key,
      price = EXCLUDED.price,
      description = EXCLUDED.description,
      images_url = EXCLUDED.images_url,
      loaded_at = NOW();
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def main():
    parser = argparse.ArgumentParser(
        description="Lab1: Load products_*.json (Project02) into PostgreSQL."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Thư mục chứa products_*.json (mặc định: tự tìm trong ./output hoặc ./data).",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="products_*.json",
        help="Glob pattern để match file (mặc định: products_*.json).",
    )
    parser.add_argument(
        "--ddl",
        type=Path,
        default=Path("sql/lab1_create_tiki_products.sql"),
        help="Đường dẫn file DDL tạo bảng.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Số record insert mỗi batch.",
    )
    args = parser.parse_args()

    load_dotenv()

    if args.input_dir is None:
        found = None
        for d in DEFAULT_INPUT_DIRS:
            p = Path(d)
            if p.exists() and p.is_dir():
                found = p
                break
        if found is None:
            raise SystemExit(
                "Không tìm thấy thư mục input. Hãy chỉ định --input-dir (ví dụ: --input-dir output)."
            )
        input_dir = found
    else:
        input_dir = args.input_dir

    files = iter_product_files(input_dir, args.pattern)
    if not files:
        raise SystemExit(f"Không tìm thấy file nào khớp pattern trong {input_dir}.")

    print(f"Đọc dữ liệu từ: {input_dir} ({len(files)} files)")

    conn = connect_postgres()
    try:
        ensure_table(conn, args.ddl)

        total_inserted = 0
        total_files = 0
        for fp in files:
            total_files += 1
            products = read_products_from_file(fp)

            inserted_this_file = 0
            for batch in batched(products, args.batch_size):
                inserted_this_file += upsert_products(conn, batch)

            total_inserted += inserted_this_file
            print(f"- {fp.name}: upsert {inserted_this_file} rows")

        print(f"Hoàn tất. Total upserted rows: {total_inserted} from {total_files} files.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

