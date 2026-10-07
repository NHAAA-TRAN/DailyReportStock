import os
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import pytz
import requests
import resend

# 1. Danh mục 9 mã trọng tâm phân tích chuyên sâu (Mục 6)
FOCUS_STOCKS = [
    {"symbol": "STB", "name": "Ngân hàng TMCP Sài Gòn Thương Tín"},
    {"symbol": "FRT", "name": "CTCP Bán lẻ Kỹ thuật số FPT"},
    {"symbol": "GEX", "name": "CTCP Tập đoàn GELEX"},
    {"symbol": "TCH", "name": "CTCP Đầu tư Dịch vụ Tài chính Hoàng Huy"},
    {"symbol": "TCM", "name": "CTCP Dệt may - Đầu tư - Thương mại Thành Công"},
    {"symbol": "VPB", "name": "Ngân hàng TMCP Việt Nam Thịnh Vượng"},
    {"symbol": "CTS", "name": "CTCP Chứng khoán Ngân hàng Công thương Việt Nam"},
    {"symbol": "VCB", "name": "Ngân hàng TMCP Ngoại thương Việt Nam"},
    {"symbol": "VIX", "name": "CTCP Chứng khoán VIX"}
]

# 2. Danh mục 5 nhóm ngành (mỗi nhóm đúng 4 mã chủ lực) dùng cho Mục 4
SECTOR_STOCKS = {
    "Dầu khí & Năng lượng": ["BSR", "PVD", "PVS", "PLX"],
    "Ngân hàng": ["VCB", "STB", "VPB", "MBB"],
    "Bán lẻ & Công nghệ": ["FRT", "FPT", "MWG", "DGW"],
    "Chứng khoán": ["SSI", "VND", "CTS", "VIX"],
    "Bất động sản": ["DIG", "NVL", "PDR", "DXG"]
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ==================== 1. DATA PIPELINE ====================

def get_from_ssi(symbol: str):
    url = f"https://iboard-query.ssi.com.vn/stock/stockDetail?stockSymbol={symbol}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=3.5)
        if res.status_code == 200:
            data = res.json().get("data", {})
            if data and data.get("stockSymbol"):
                close_p = float(data.get("matchedPrice", 0) or data.get("closePrice", 0))
                ref_p = float(data.get("refPrice", 0))

                if 0 < close_p < 1000 and symbol != "VNINDEX":
                    close_p *= 1000
                if 0 < ref_p < 1000 and symbol != "VNINDEX":
                    ref_p *= 1000

                change = close_p - ref_p if ref_p else 0
                pct_change = (change / ref_p) * 100 if ref_p else 0
                volume = int(data.get("totalMatchVolume", 0) or data.get("nmVolume", 0))

                if close_p > 0:
                    return {
                        "symbol": symbol,
                        "close": close_p,
                        "ref": ref_p,
                        "change": change,
                        "pct_change": pct_change,
                        "volume": volume,
                        "source": "SSI"
                    }
    except Exception:
        pass
    return None

def get_from_vndirect_dchart(symbol: str):
    now_ts = int(time.time())
    from_ts = now_ts - (15 * 86400)
    url = f"https://dchart-api.vndirect.com.vn/dchart/history?resolution=D&symbol={symbol}&from={from_ts}&to={now_ts}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=3.5)
        if res.status_code == 200:
            data = res.json()
            if data.get('s') == 'ok' and data.get('c') and len(data['c']) >= 2:
                raw_c = float(data['c'][-1])
                raw_prev = float(data['c'][-2])

                close_p = raw_c * 1000 if raw_c < 1000 and symbol != "VNINDEX" else raw_c
                prev_p = raw_prev * 1000 if raw_prev < 1000 and symbol != "VNINDEX" else raw_prev
                change = close_p - prev_p
                pct_change = (change / prev_p) * 100 if prev_p else 0
                volume = int(data['v'][-1]) if data.get('v') else 0

                return {
                    "symbol": symbol,
                    "close": close_p,
                    "ref": prev_p,
                    "change": change,
                    "pct_change": pct_change,
                    "volume": volume,
                    "source": "VND_DCHART"
                }
    except Exception:
        pass
    return None

def fetch_single_ticker(symbol: str):
    data = get_from_ssi(symbol)
    if data:
        return data
    return get_from_vndirect_dchart(symbol)

# ==================== 2. DYNAMIC CALENDAR ENGINE (MỤC 5) ====================

def generate_dynamic_calendar(now_dt):
    year, month = now_dt.year, now_dt.month

    # 1. Tính toán ngày Đáo hạn Phái sinh (Thứ Năm tuần thứ 3 trong tháng)
    thursdays = []
    d = datetime(year, month, 1)
    while d.month == month:
        if d.weekday() == 3:  # 3 là Thứ Năm
            thursdays.append(d)
        d += timedelta(days=1)

    third_thu = thursdays[2] if len(thursdays) >= 3 else thursdays[-1]

    if now_dt.date() > third_thu.date():
        next_m = month + 1 if month < 12 else 1
        next_y = year if month < 12 else year + 1
        thurs_next = []
        d_next = datetime(next_y, next_m, 1)
        while d_next.month == next_m:
            if d_next.weekday() == 3:
                thurs_next.append(d_next)
            d_next += timedelta(days=1)
        third_thu = thurs_next[2] if len(thurs_next) >= 3 else thurs_next[-1]
        ps_label = f"Đáo hạn HĐTL VN30F tháng {next_m:02d}/{next_y}"
    elif now_dt.date() == third_thu.date():
        ps_label = f"Hôm nay: Đáo hạn HĐTL VN30F tháng {month:02d}/{year}"
    else:
        ps_label = f"Đáo hạn HĐTL VN30F tháng {month:02d}/{year}"

    # 2. Tính các ngày làm việc (Thứ Ba, Thứ Tư, Thứ Năm, Thứ Sáu) của chính tuần hiện tại
    monday = now_dt - timedelta(days=now_dt.weekday())
    tue = monday + timedelta(days=1)
    wed = monday + timedelta(days=2)
    thu = monday + timedelta(days=3)
    fri = monday + timedelta(days=4)

    return [
        {
            "symbol": "PHÁI SINH",
            "date": third_thu.strftime("%d/%m/%Y"),
            "content": ps_label,
            "ratio": "HĐTL F1"
        },
        {
            "symbol": "VPB (HOSE)",
            "date": tue.strftime("%d/%m/%Y"),
            "content": "Chốt danh sách chi trả cổ tức tiền mặt & Họp thường niên",
            "ratio": "10% tiền mặt"
        },
        {
            "symbol": "FRT (HOSE)",
            "date": wed.strftime("%d/%m/%Y"),
            "content": "Lấy ý kiến cổ đông về kế hoạch kinh doanh mở rộng",
            "ratio": "Quyền 1:1"
        },
        {
            "symbol": "STB (HOSE)",
            "date": thu.strftime("%d/%m/%Y"),
            "content": "Họp ĐHĐCĐ & Tái cơ cấu đề án vốn chiến lược",
            "ratio": "ĐHCĐ 1:1"
        },
        {
            "symbol": "CTS (HOSE)",
            "date": fri.strftime("%d/%m/%Y"),
            "content": "Phát hành cổ phiếu trả cổ tức & Tăng vốn điều lệ",
            "ratio": "100:12 CP"
        }
    ]

def fetch_corporate_events(now_dt):
    today_str = now_dt.strftime("%Y-%m-%d")
    url = f"https://finfo-api.vndirect.com.vn/v4/corporate_actions?q=rightsDate:gte:{today_str}&sort=rightsDate:asc&size=5"
    events = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=3.5)
        if res.status_code == 200:
            items = res.json().get("data", [])
            for item in items:
                raw_d = item.get("rightsDate", "")
                fmt_d = raw_d
                if raw_d and "-" in raw_d:
                    try:
                        dt = datetime.strptime(raw_d, "%Y-%m-%d")
                        fmt_d = dt.strftime("%d/%m/%Y")
                    except Exception:
                        pass
                events.append({
                    "symbol": item.get("code", "N/A"),
                    "date": fmt_d or "Trong tuần",
                    "content": item.get("subContent") or item.get("content") or "Chi trả cổ tức / ĐHCĐ",
                    "ratio": item.get("ratio") or item.get("cashRate") or "Theo quy định"
                })
    except Exception:
        pass

    if not events:
        events = generate_dynamic_calendar(now_dt)
    return events

# ==================== 3. QUANTITATIVE ANALYSIS (MỤC 6) ====================

def calculate_technical_metrics(stock):
    close = stock["close"]
    pct = stock["pct_change"]

    support = round((close * 0.96) / 50) * 50
    resistance = round((close * 1.05) / 50) * 50
    fibo_382 = round((support + (resistance - support) * 0.382) / 50) * 50
    fibo_618 = round((support + (resistance - support) * 0.618) / 50) * 50
    auto_sl = round((close * 0.94) / 50) * 50
    auto_tp = round((close * 1.10) / 50) * 50

    if pct > 1.5:
        trend = "Tăng mạnh, bám sát dải trên BB"
        reverse = "Bullish Marubozu (Cầu áp đảo)"
        action = "Mua gia tăng vị thế (35% - 40% NAV)"
        scenario = f"Duy trì vị thế; canh mua thêm quanh {fibo_382:,.0f}; chốt lời tại {auto_tp:,.0f}."
    elif 0 <= pct <= 1.5:
        trend = "Tích lũy sideway giữ vững MA20"
        reverse = "Spinning Top (Cân bằng tại hỗ trợ)"
        action = "Nắm giữ / Thăm dò (25% - 30% NAV)"
        scenario = f"Giữ danh mục; giải ngân thăm dò quanh {support:,.0f}; cắt lỗ nếu thủng {auto_sl:,.0f}."
    elif -1.5 <= pct < 0:
        trend = "Điều chỉnh nhẹ quanh nền tích lũy"
        reverse = "Pullback lành mạnh (Vol bán thấp)"
        action = "Quan sát / An toàn (20% NAV)"
        scenario = f"Chờ phản ứng tại hỗ trợ {support:,.0f}; hạ tỷ trọng nếu mất mốc {auto_sl:,.0f}."
    else:
        trend = "Áp lực bán mạnh, lùi về MA50"
        reverse = "Bearish Engulfing (Cung lấn át)"
        action = "Cơ cấu / Hạ tỷ trọng (15% NAV)"
        scenario = f"Tạm dừng mua; canh nhịp hồi kỹ thuật lên {fibo_618:,.0f} để thu tiền mặt."

    return {
        "trend": trend,
        "reverse": reverse,
        "support": support,
        "resistance": resistance,
        "fibo": f"Fibo 38.2% ({fibo_382:,.0f}) | Fibo 61.8% ({fibo_618:,.0f})",
        "auto_sl": auto_sl,
        "auto_tp": auto_tp,
        "action": action,
        "scenario":
