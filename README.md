# Tiki Product Fetcher (Project 2)

Tải thông tin ~200k sản phẩm Tiki từ API, **tách pipeline 2 stage** để dễ debug và vận hành ổn định:

- **Stage 1 – Raw Crawl**: crawl & lưu raw JSON từ API (`raw_products_*.json`)
- **Stage 2 – Clean & Format**: đọc raw, chuẩn hoá và ghi output sạch (`products_*.json`)

## Yêu cầu

- Python 3.10+
- `beautifulsoup4`: chuẩn hoá `description` (bỏ HTML, gộp khoảng trắng)
- Khuyến nghị `aiohttp`: chạy async nhanh hơn
- `ujson` (optional): tăng tốc xử lý JSON (script sẽ fallback về `json` nếu không có)
- `pytest`: chạy unit test data quality (unique ID)

## Cài đặt

```bash
pip install -r requirements.txt
```

## Dữ liệu đầu vào (product_id)

- File `product_ids.txt` không commit (dung lượng lớn).
- Có thể dùng OneDrive share link theo đề bài hoặc tự chuẩn bị danh sách ID.

Định dạng:

- Mỗi dòng 1 `product_id` (số), hoặc CSV với `product_id` ở cột đầu.
- Bỏ qua dòng trống và dòng bắt đầu bằng `#`.

## Chạy pipeline

### Cách 1: Dùng `entrypoint.sh` (auto-restart)

```bash
chmod +x entrypoint.sh
IDS_FILE=product_ids.txt OUTPUT_DIR=output RETRY_DELAY=10 ./entrypoint.sh
```

### Cách 2: Chạy từng stage

**Stage 1 – Raw crawl (có throttling + checkpoint):**

```bash
python fetch_tiki_products.py --ids-file product_ids.txt --output-dir ./output
```

**Stage 2 – Clean & format:**

```bash
python clean_tiki_products.py --raw-dir ./output --output-dir ./output
```

## Throttling & checkpoint

- **Throttling**: giới hạn tốc độ để ổn định lâu dài (mặc định ~50 req/s).
- **Checkpoint**: `checkpoint_processed_ids.txt` lưu các `id` đã crawl xong; chạy lại sẽ tự skip các ID đã xử lý.

## Output

- Stage 1: `raw_products_0001.json`, `raw_products_0002.json`, ...
- Stage 2: `products_0001.json`, `products_0002.json`, ...

## Unit test: đảm bảo ID duy nhất

Sau khi chạy Stage 2 (có `products_*.json` trong `output/`), chạy:

```bash
pytest tests/test_unique_ids.py
```

