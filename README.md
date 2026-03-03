# Tiki Product Fetcher

Tải thông tin ~200k sản phẩm Tiki từ API, chuẩn hoá mô tả, lưu thành các file JSON (mỗi file ~1000 sản phẩm).

## Yêu cầu

- Python 3.10+
- **Bắt buộc:** `beautifulsoup4` (chuẩn hoá description).
- **Khuyến nghị:** `aiohttp` (gọi API đồng thời, nhanh hơn nhiều). Nếu không cài, script dùng `ThreadPoolExecutor` + `urllib` (chậm hơn).

## Cài đặt

```bash
pip install -r requirements.txt
```

## Dữ liệu đầu vào (product_id)

- File `product_ids.txt` **không** được đính kèm trong repo do dung lượng lớn (~200k ID).
- Danh sách ID được cung cấp qua link OneDrive của đề bài:  
  `https://1drv.ms/u/s!AukvlU4z92FZgp4xIlzQ4giHVa5Lpw?e=qDXctn`
- Người dùng tự tải file từ OneDrive, hoặc tự chuẩn bị danh sách ID của riêng mình.

Định dạng file:

- Mỗi dòng một `product_id` (số), hoặc CSV với `product_id` ở cột đầu.
- Có thể bỏ qua dòng trống và dòng bắt đầu bằng `#`.

## Chạy

**Dùng file danh sách local (khuyến nghị):**
```bash
python fetch_tiki_products.py --ids-file product_ids.txt
```

**Tuỳ chọn:**
- `--output-dir`: thư mục lưu file JSON (mặc định: `./output`)
- `--per-file`: số sản phẩm mỗi file (mặc định: 1000)
- `--concurrency`: số request đồng thời (mặc định: 150, tăng để nhanh hơn nếu API chấp nhận)

Ví dụ:
```bash
python fetch_tiki_products.py --ids-file product_ids.txt --output-dir ./data --concurrency 200
```

## Dữ liệu lấy được

Mỗi sản phẩm trong JSON gồm:

| Trường | Mô tả |
|--------|--------|
| `id` | ID sản phẩm |
| `name` | Tên sản phẩm |
| `url_key` | Slug URL |
| `price` | Giá (VNĐ) |
| `description` | Mô tả đã chuẩn hoá (bỏ HTML, gộp khoảng trắng) |
| `images_url` | Danh sách URL ảnh (ưu tiên ảnh lớn) |

## Chuẩn hoá description

- Loại bỏ toàn bộ thẻ HTML.
- Lấy nội dung text, thay khoảng trắng/nhảy dòng liên tiếp bằng một dấu cách.
- Decode HTML entities (BeautifulSoup xử lý).

## Rút ngắn thời gian

- Dùng **asyncio + aiohttp** gọi API đồng thời (mặc định 150 request cùng lúc).
- Có thể tăng `--concurrency` (ví dụ 200–300) nếu API Tiki không chặn; nếu bị 429/block thì giảm xuống.

## Output

- Thư mục: `output/` (hoặc theo `--output-dir`).
- Tên file: `products_0001.json`, `products_0002.json`, ... (mỗi file tối đa 1000 sản phẩm).
