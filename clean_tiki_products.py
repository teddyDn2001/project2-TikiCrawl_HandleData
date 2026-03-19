#!/usr/bin/env python3
"""
Stage 2: Đọc dữ liệu raw từ Stage 1, chuẩn hoá và ghi lại thành products_XXXX.json.
"""

import argparse
import gc
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from fetch_tiki_products import (
    build_product_record,
    json_loads,
    json_dumps,
    OUTPUT_DIR,
    RAW_OUTPUT_PREFIX,
    PRODUCTS_PER_FILE,
    LOGS_DIR,
    setup_logging,
    atomic_write_text,
)


def clean_raw_files(
    raw_dir: Path,
    output_dir: Path,
    products_per_file: int = PRODUCTS_PER_FILE,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    all_clean: list[dict] = []
    file_index = 0
    logger = logging.getLogger("tiki_fetcher")

    try:
        from tqdm import tqdm  # type: ignore
    except Exception:
        tqdm = None  # type: ignore

    raw_files = sorted(raw_dir.glob(f"{RAW_OUTPUT_PREFIX}_*.json"))
    if not raw_files:
        logger.error("Không tìm thấy file raw nào trong thư mục: %s", raw_dir)
        return

    it = raw_files
    if tqdm is not None:
        it = tqdm(raw_files, desc="Stage2 raw files", unit="file")  # type: ignore

    for raw_path in it:  # type: ignore
        logger.info("Đang xử lý file raw: %s", raw_path)
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_data = json_loads(f.read())

        for item in raw_data:
            if not isinstance(item, dict):
                continue
            rec = build_product_record(item)
            if rec is not None:
                all_clean.append(rec)

            while len(all_clean) >= products_per_file:
                file_index += 1
                current_batch = all_clean[:products_per_file]
                out_path = output_dir / f"products_{file_index:04d}.json"
                atomic_write_text(out_path, json_dumps(current_batch))
                all_clean = all_clean[products_per_file:]
                logger.info("Đã ghi file sạch: %s", out_path)

        # giải phóng bộ nhớ sớm
        del raw_data
        gc.collect()

    if all_clean:
        file_index += 1
        out_path = output_dir / f"products_{file_index:04d}.json"
        atomic_write_text(out_path, json_dumps(all_clean))
        logger.info("Đã ghi file sạch cuối cùng: %s", out_path)

    logger.info("Stage 2 (clean & format) hoàn tất.")


def main():
    load_dotenv()
    setup_logging(LOGS_DIR / "stage2.log")
    parser = argparse.ArgumentParser(description="Stage 2: Chuẩn hoá dữ liệu raw thành products_*.json.")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Thư mục chứa raw_products_*.json (mặc định: ./output).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Thư mục lưu products_*.json sau khi clean (mặc định: ./output).",
    )
    parser.add_argument(
        "--per-file",
        type=int,
        default=PRODUCTS_PER_FILE,
        help=f"Số sản phẩm mỗi file JSON sạch (mặc định: {PRODUCTS_PER_FILE}).",
    )
    args = parser.parse_args()

    try:
        clean_raw_files(
            raw_dir=args.raw_dir,
            output_dir=args.output_dir,
            products_per_file=args.per_file,
        )
    except MemoryError:
        logging.getLogger("tiki_fetcher").error("Stage 2 MemoryError (thiếu RAM). Hãy giảm --per-file hoặc chạy máy nhiều RAM.")
        sys.exit(1)


if __name__ == "__main__":
    main()

