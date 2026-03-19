#!/usr/bin/env python3
"""
Tải thông tin ~200k sản phẩm Tiki từ API, chuẩn hoá description, lưu JSON (1000 sản phẩm/file).
- Đọc list product_id từ file local hoặc từ OneDrive share link.
- Gọi API đồng thời (asyncio + aiohttp) để rút ngắn thời gian.
- Chuẩn hoá description: bỏ HTML, gộp khoảng trắng, decode entities.
"""

import argparse
import base64
import logging
import os
import re
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

try:
    import orjson as _json_impl

    def json_loads(s: str | bytes):
        return _json_impl.loads(s)

    def json_dumps(obj) -> str:
        return _json_impl.dumps(obj).decode("utf-8")

except ImportError:
    try:
        import ujson as _json_impl  # type: ignore[no-redef]

        def json_loads(s: str | bytes):
            return _json_impl.loads(s)

        def json_dumps(obj) -> str:
            # ujson không hỗ trợ ensure_ascii, trả luôn str
            return _json_impl.dumps(obj)

    except ImportError:
        import json as _json_impl  # type: ignore[no-redef]

        def json_loads(s: str | bytes):
            return _json_impl.loads(s)

        def json_dumps(obj) -> str:
            return _json_impl.dumps(obj, ensure_ascii=False)

try:    
    import aiohttp
    import asyncio
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

import urllib.request
import urllib.error
from bs4 import BeautifulSoup

# --- Cấu hình ---
API_BASE = "https://api.tiki.vn/product-detail/api/v1/products"
PRODUCTS_PER_FILE = 1000
CONCURRENT_REQUESTS = 150
REQUEST_TIMEOUT = 30
MAX_REQUESTS_PER_SECOND = 50
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
RAW_OUTPUT_PREFIX = "raw_products"
CHECKPOINT_FILE = Path(__file__).resolve().parent / "checkpoint_processed_ids.txt"
ONEDRIVE_SHARE_URL = "https://1drv.ms/u/s!AukvlU4z92FZgp4xIlzQ4giHVa5Lpw?e=qDXctn"

LOGS_DIR = Path(__file__).resolve().parent / "logs"


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v.strip() else default


def setup_logging(log_file: Path) -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("tiki_fetcher")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger


def atomic_write_text(path: Path, content: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def normalize_description(html: str | None) -> str:
    """Chuẩn hoá nội dung description: bỏ HTML, decode entities, gộp khoảng trắng."""
    if not html or not html.strip():
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_images_urls(images: list | None) -> list[str]:
    """Lấy danh sách URL ảnh từ dữ liệu API."""
    if not images:
        return []
    urls = []
    for img in images:
        url = img.get("large_url") or img.get("base_url") or img.get("medium_url") or img.get("small_url")
        if url:
            urls.append(url)
    return urls


def build_product_record(raw: dict) -> dict | None:
    """Tạo bản ghi sản phẩm đã chuẩn hoá.

    Hàm này được dùng ở Stage 2 (cleaning), có thể import từ script khác.
    """
    try:
        product_id = raw.get("id")
        price = raw.get("price")
        if product_id is None or price is None:
            # Bắt buộc phải có id và price, nếu thiếu thì bỏ qua.
            return None

        return {
            "id": product_id,
            "name": raw.get("name"),
            "url_key": raw.get("url_key"),
            "price": price,
            "description": normalize_description(raw.get("description")),
            "images_url": extract_images_urls(raw.get("images")),
        }
    except Exception:
        return None


def _fetch_one_sync(product_id: int | str) -> dict | None:
    """Gọi API một sản phẩm (chế độ sync), trả về raw JSON."""
    url = f"{API_BASE}/{product_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; TikiProductFetcher/1.0)"})
    max_retries = _env_int("MAX_RETRIES", 5)
    backoff_base = float(os.getenv("BACKOFF_BASE_SECONDS", "2"))
    backoff_cap = float(os.getenv("BACKOFF_CAP_SECONDS", "30"))

    for attempt in range(max_retries + 1):
        try:
            # Throttling đơn giản theo MAX_REQUESTS_PER_SECOND
            time.sleep(1 / MAX_REQUESTS_PER_SECOND)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                if getattr(resp, "status", 200) != 200:
                    return None
                try:
                    data = json_loads(resp.read())
                except Exception:
                    return None
                return data
        except urllib.error.HTTPError as e:
            # Exponential backoff cho 429
            if e.code == 429 and attempt < max_retries:
                sleep_s = min(backoff_cap, backoff_base * (2**attempt))
                time.sleep(sleep_s)
                continue
            return None
        except (urllib.error.URLError, TimeoutError):
            if attempt < max_retries:
                time.sleep(min(backoff_cap, backoff_base * (2**attempt)))
                continue
            return None
        except Exception:
            return None


if HAS_AIOHTTP:
    async def fetch_one(
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        product_id: int | str,
    ) -> dict | None:
        """Gọi API một sản phẩm (async, có giới hạn đồng thời), trả về raw JSON."""
        url = f"{API_BASE}/{product_id}"
        max_retries = _env_int("MAX_RETRIES", 5)
        backoff_base = float(os.getenv("BACKOFF_BASE_SECONDS", "2"))
        backoff_cap = float(os.getenv("BACKOFF_CAP_SECONDS", "30"))
        async with sem:
            for attempt in range(max_retries + 1):
                try:
                    # Throttling đơn giản theo MAX_REQUESTS_PER_SECOND
                    await asyncio.sleep(1 / MAX_REQUESTS_PER_SECOND)
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                        if resp.status == 429 and attempt < max_retries:
                            sleep_s = min(backoff_cap, backoff_base * (2**attempt))
                            await asyncio.sleep(sleep_s)
                            continue
                        if resp.status != 200:
                            return None
                        text = await resp.text()
                        try:
                            data = json_loads(text)
                        except Exception:
                            return None
                        return data
                except asyncio.TimeoutError:
                    if attempt < max_retries:
                        await asyncio.sleep(min(backoff_cap, backoff_base * (2**attempt)))
                        continue
                    return None
                except aiohttp.ClientConnectionError:
                    if attempt < max_retries:
                        await asyncio.sleep(min(backoff_cap, backoff_base * (2**attempt)))
                        continue
                    return None
                except aiohttp.ClientError:
                    return None


def load_checkpoint(path: Path = CHECKPOINT_FILE) -> set[str]:
    """Đọc danh sách product_id đã xử lý từ checkpoint."""
    if not path.exists():
        return set()
    processed: set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                processed.add(line)
    return processed


def append_checkpoint(ids: list[str], path: Path = CHECKPOINT_FILE) -> None:
    """Ghi thêm danh sách product_id đã xử lý vào checkpoint."""
    if not ids:
        return
    with open(path, "a", encoding="utf-8") as f:
        for pid in ids:
            f.write(f"{pid}\n")


def load_product_ids_from_file(path: Path) -> list[str]:
    """Đọc danh sách product ID từ file."""
    ids = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Nếu dòng là CSV, lấy cột đầu
            parts = line.split(",")
            raw = parts[0].strip()
            if raw.isdigit():
                ids.append(raw)
    return ids


def onedrive_share_to_direct_url(share_url: str) -> str:
    """Chuyển OneDrive share link sang URL tải trực tiếp."""
    import urllib.parse
    b64 = base64.b64encode(share_url.encode("utf-8")).decode("ascii")
    # Unpadded base64url
    b64 = b64.rstrip("=").replace("/", "_").replace("+", "-")
    return f"https://api.onedrive.com/v1.0/shares/u!{b64}/root/content"


def _parse_ids_from_text(text: str) -> list[str]:
    ids = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.replace("\t", ",").split(",")
        raw = parts[0].strip()
        if raw.isdigit():
            ids.append(raw)
    return ids


def download_product_ids_from_onedrive_sync(share_url: str) -> list[str]:
    """Tải danh sách product ID từ OneDrive (sync)."""
    direct = onedrive_share_to_direct_url(share_url)
    req = urllib.request.Request(direct, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        if resp.status != 200:
            raise RuntimeError(f"OneDrive trả về status {resp.status}. Hãy tải file thủ công và dùng --ids-file.")
        text = resp.read().decode()
    return _parse_ids_from_text(text)


async def download_product_ids_from_onedrive(share_url: str) -> list[str]:
    """Tải danh sách product ID từ OneDrive (async)."""
    direct = onedrive_share_to_direct_url(share_url)
    async with aiohttp.ClientSession() as session:
        async with session.get(direct, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"OneDrive trả về status {resp.status}. Hãy tải file thủ công và dùng --ids-file.")
            text = await resp.text()
    return _parse_ids_from_text(text)


def run_sync(
    product_ids: list[str],
    output_dir: Path = OUTPUT_DIR,
    products_per_file: int = PRODUCTS_PER_FILE,
    concurrency: int = CONCURRENT_REQUESTS,
):
    """Stage 1: Crawl raw data bằng ThreadPoolExecutor (khi không có aiohttp)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    batch: list[dict] = []
    file_index = 0
    total = len(product_ids)
    done = 0
    logger = logging.getLogger("tiki_fetcher")
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(_fetch_one_sync, pid): pid for pid in product_ids}
        for future in as_completed(futures):
            r = future.result()
            if r is not None:
                batch.append(r)
            done += 1
            if done % 500 == 0 or done == total:
                logger.info("Đã xử lý: %s/%s", done, total)
            while len(batch) >= products_per_file:
                file_index += 1
                current_batch = batch[:products_per_file]
                out_path = output_dir / f"{RAW_OUTPUT_PREFIX}_{file_index:04d}.json"
                atomic_write_text(out_path, json_dumps(current_batch))
                # Cập nhật checkpoint cho batch này
                processed_ids = [
                    str(item.get("id"))
                    for item in current_batch
                    if isinstance(item, dict) and item.get("id") is not None
                ]
                append_checkpoint(processed_ids)
                batch = batch[products_per_file:]
                logger.info("Đã ghi: %s", out_path)
    if batch:
        file_index += 1
        current_batch = batch
        out_path = output_dir / f"{RAW_OUTPUT_PREFIX}_{file_index:04d}.json"
        atomic_write_text(out_path, json_dumps(current_batch))
        processed_ids = [
            str(item.get("id"))
            for item in current_batch
            if isinstance(item, dict) and item.get("id") is not None
        ]
        append_checkpoint(processed_ids)
        logger.info("Đã ghi: %s", out_path)
    logger.info("Stage 1 (raw crawl, sync) hoàn tất.")


async def run(
    product_ids: list[str],
    output_dir: Path = OUTPUT_DIR,
    products_per_file: int = PRODUCTS_PER_FILE,
    concurrency: int = CONCURRENT_REQUESTS,
):
    """Stage 1: Crawl raw data bằng asyncio + aiohttp."""
    output_dir.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(concurrency)
    batch: list[dict] = []
    file_index = 0
    logger = logging.getLogger("tiki_fetcher")

    try:
        from tqdm import tqdm  # type: ignore
    except Exception:
        tqdm = None  # type: ignore

    async with aiohttp.ClientSession(
        headers={"User-Agent": "Mozilla/5.0 (compatible; TikiProductFetcher/1.0)"}
    ) as session:
        total = len(product_ids)
        done = 0
        iterator = range(0, total, concurrency * 2)
        if tqdm is not None:
            iterator = tqdm(iterator, desc="Stage1 chunks", unit="chunk")  # type: ignore
        for i in iterator:  # type: ignore
            chunk_ids = product_ids[i: i + concurrency * 2]
            tasks = [fetch_one(session, sem, pid) for pid in chunk_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    continue
                if r is not None:
                    batch.append(r)
                done += 1
                if done % 500 == 0 or done == total:
                    logger.info("Đã xử lý: %s/%s", done, total)
                while len(batch) >= products_per_file:
                    file_index += 1
                    current_batch = batch[:products_per_file]
                    out_path = output_dir / f"{RAW_OUTPUT_PREFIX}_{file_index:04d}.json"
                    atomic_write_text(out_path, json_dumps(current_batch))
                    processed_ids = [
                        str(item.get("id"))
                        for item in current_batch
                        if isinstance(item, dict) and item.get("id") is not None
                    ]
                    append_checkpoint(processed_ids)
                    batch = batch[products_per_file:]
                    logger.info("Đã ghi: %s", out_path)
        if batch:
            file_index += 1
            current_batch = batch
            out_path = output_dir / f"{RAW_OUTPUT_PREFIX}_{file_index:04d}.json"
            atomic_write_text(out_path, json_dumps(current_batch))
            processed_ids = [
                str(item.get("id"))
                for item in current_batch
                if isinstance(item, dict) and item.get("id") is not None
            ]
            append_checkpoint(processed_ids)
            logger.info("Đã ghi: %s", out_path)
    logger.info("Stage 1 (raw crawl, async) hoàn tất.")


def main():
    load_dotenv()
    global PRODUCTS_PER_FILE, CONCURRENT_REQUESTS, REQUEST_TIMEOUT, MAX_REQUESTS_PER_SECOND
    PRODUCTS_PER_FILE = _env_int("PRODUCTS_PER_FILE", PRODUCTS_PER_FILE)
    CONCURRENT_REQUESTS = _env_int("CONCURRENT_REQUESTS", CONCURRENT_REQUESTS)
    REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", REQUEST_TIMEOUT)
    MAX_REQUESTS_PER_SECOND = _env_int("MAX_REQUESTS_PER_SECOND", MAX_REQUESTS_PER_SECOND)

    logger = setup_logging(LOGS_DIR / "stage1.log")
    logger.info("Khởi động Stage 1 (raw crawl)")

    parser = argparse.ArgumentParser(description="Tải thông tin sản phẩm Tiki, lưu JSON.")
    parser.add_argument(
        "--ids-file",
        type=Path,
        default=None,
        help="Đường dẫn file chứa product ID (mỗi dòng 1 ID). Nếu không chỉ định, thử tải từ OneDrive.",
    )
    parser.add_argument(
        "--onedrive-url",
        type=str,
        default=ONEDRIVE_SHARE_URL,
        help="OneDrive share link chứa file danh sách product ID.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Thư mục lưu file JSON (mặc định: {OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--per-file",
        type=int,
        default=PRODUCTS_PER_FILE,
        help=f"Số sản phẩm mỗi file JSON (mặc định: {PRODUCTS_PER_FILE}).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=CONCURRENT_REQUESTS,
        help=f"Số request đồng thời (mặc định: {CONCURRENT_REQUESTS}).",
    )
    args = parser.parse_args()

    if args.ids_file and args.ids_file.exists():
        product_ids = load_product_ids_from_file(args.ids_file)
        logger.info("Đã đọc %s product ID từ file: %s", len(product_ids), args.ids_file)
    else:
        if args.ids_file:
            logger.warning("File không tồn tại: %s. Thử tải từ OneDrive...", args.ids_file)
        try:
            if HAS_AIOHTTP:
                product_ids = asyncio.run(download_product_ids_from_onedrive(args.onedrive_url))
            else:
                product_ids = download_product_ids_from_onedrive_sync(args.onedrive_url)
            logger.info("Đã tải %s product ID từ OneDrive.", len(product_ids))
        except Exception as e:
            logger.error("Lỗi tải OneDrive: %s", e)
            sys.exit(1)

    if not product_ids:
        logger.error("Không có product ID nào.")
        sys.exit(1)

    # Áp dụng checkpoint: bỏ qua những ID đã xử lý ở lần chạy trước
    processed_ids = load_checkpoint()
    if processed_ids:
        before = len(product_ids)
        product_ids = [pid for pid in product_ids if pid not in processed_ids]
        after = len(product_ids)
        skipped = before - after
        logger.info("Checkpoint: bỏ qua %s ID đã xử lý, còn lại %s ID cần crawl.", skipped, after)
    else:
        logger.info("Checkpoint: chưa có file checkpoint, crawl toàn bộ danh sách ID.")

    if HAS_AIOHTTP:
        asyncio.run(
            run(
                product_ids,
                output_dir=args.output_dir,
                products_per_file=args.per_file,
                concurrency=args.concurrency,
            )
        )
    else:
        logger.warning("Chạy ở chế độ sync với ThreadPoolExecutor vì chưa cài aiohttp. Cài aiohttp để nhanh hơn.")
        run_sync(
            product_ids,
            output_dir=args.output_dir,
            products_per_file=args.per_file,
            concurrency=args.concurrency,
        )


if __name__ == "__main__":
    main()