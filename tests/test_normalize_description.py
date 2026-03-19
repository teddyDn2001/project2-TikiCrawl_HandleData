from fetch_tiki_products import normalize_description


def test_normalize_description_strips_html_nested():
    html = "<div>Hello <b>world</b><p>Line<br>break</p></div>"
    out = normalize_description(html)
    assert out == "Hello world Line break"


def test_normalize_description_handles_weird_chars():
    # chuỗi có ký tự lạ / lỗi font (replacement char) vẫn không làm crash
    html = "<p>Xin chào\uFFFD\uFFFD <span>Việt Nam</span></p>"
    out = normalize_description(html)
    assert "Xin chào" in out
    assert "Việt Nam" in out

