#!/usr/bin/env python3
"""
Stage 2: Đọc dữ liệu raw từ Stage 1, chuẩn hoá và ghi lại thành products_XXXX.json.
"""

import argparse
from pathlib import Path

from fetch_tiki_products import build_product_record, json_loads, json_dumps, OUTPUT_DIR, RAW_OUTPUT_PREFIX, PRODUCTS_PER_FILE


def clean_raw_files(
    raw_dir: Path,
    output_dir: Path,
    products_per_file: int = PRODUCTS_PER_FILE,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    all_clean: list[dict] = []
    file_index = 0

    raw_files = sorted(raw_dir.glob(f"{RAW_OUTPUT_PREFIX}_*.json"))
    if not raw_files:
        print(f"Không tìm thấy file raw nào trong thư mục: {raw_dir}")
        return

    for raw_path in raw_files:
        print(f"Đang xử lý file raw: {raw_path}")
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
                with open(out_path, "w", encoding="utf-8") as out_f:
                    out_f.write(json_dumps(current_batch))
                all_clean = all_clean[products_per_file:]
                print(f"Đã ghi file sạch: {out_path}")

    if all_clean:
        file_index += 1
        out_path = output_dir / f"products_{file_index:04d}.json"
        with open(out_path, "w", encoding="utf-8") as out_f:
            out_f.write(json_dumps(all_clean))
        print(f"Đã ghi file sạch cuối cùng: {out_path}")

    print("Stage 2 (clean & format) hoàn tất.")


def main():
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

    clean_raw_files(
        raw_dir=args.raw_dir,
        output_dir=args.output_dir,
        products_per_file=args.per_file,
    )


if __name__ == "__main__":
    main()

