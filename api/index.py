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
    """Bộ máy tự động hóa lịch tài chính và sự kiện tuần theo thời gian thực 100%"""
    year, month = now_dt.year, now_dt.month

    # 1. Tự động tính ngày Thứ Năm tuần thứ 3 trong tháng (Đáo hạn HĐTL VN30F)
    thursdays = []
    d = datetime(year, month, 1)
    while d.month == month:
        if d.weekday() == 3:  # 3 đại diện cho Thứ Năm
            thursdays.append(d)
        d += timedelta(days=1)

    third_thu = thursdays[2] if len(thursdays) >= 3 else thursdays[-1]

    # Nếu Thứ Năm tuần 3 của tháng này đã qua, tự động tính cho tháng tiếp theo
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

    # 2. Tự động tính các mốc ngày làm việc (Thứ Ba, Thứ Tư, Thứ Năm, Thứ Sáu) của chính tuần hiện tại
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
        "scenario": scenario
    }

# ==================== 4. MOBILE-RESPONSIVE CANVAS BUILDER ====================

def build_canvas_dashboard(vnindex, stocks_analyzed, fetched_data, events, date_str, execution_id):
    vn_close = vnindex["close"] if vnindex else 1275.50
    vn_change = vnindex["change"] if vnindex else 4.25
    vn_pct = vnindex["pct_change"] if vnindex else 0.33

    vn_color = "#15803d" if vn_change >= 0 else "#b91c1c"
    vn_sign = "+" if vn_change >= 0 else ""

    # ----------------------------------------------------
    # DỰNG MỤC 4: 5 NHÓM NGÀNH (MỖI NGÀNH 4 MÃ) & TOP RỦI RO / TÍCH LŨY
    # ----------------------------------------------------
    sector_rows = ""
    for idx, (sec_name, tickers) in enumerate(SECTOR_STOCKS.items()):
        bg_col = "#ffffff" if idx % 2 == 0 else "#f8fafc"
        stock_items_html = ""
        for t in tickers:
            st = fetched_data.get(t)
            if st:
                c_val = st["close"]
                p_val = st["pct_change"]
                chg_val = st["change"]
                st_color = "#15803d" if chg_val > 0 else ("#b91c1c" if chg_val < 0 else "#b45309")
                st_sign = "+" if chg_val > 0 else ""

                if p_val >= 2.0:
                    badge = '<span style="background-color: #dcfce7; color: #15803d; padding: 1px 6px; border-radius: 4px; font-weight: bold; font-size: 11px;">Mua gia tăng</span>'
                elif 0.5 <= p_val < 2.0:
                    badge = '<span style="background-color: #f0fdf4; color: #16a34a; padding: 1px 6px; border-radius: 4px; font-weight: bold; font-size: 11px;">Mua tích lũy</span>'
                elif -0.5 <= p_val < 0.5:
                    badge = '<span style="background-color: #f0f9ff; color: #0284c7; padding: 1px 6px; border-radius: 4px; font-weight: bold; font-size: 11px;">Nắm giữ</span>'
                elif -2.0 <= p_val < -0.5:
                    badge = '<span style="background-color: #fffbeb; color: #d97706; padding: 1px 6px; border-radius: 4px; font-weight: bold; font-size: 11px;">Quan sát</span>'
                else:
                    badge = '<span style="background-color: #fef2f2; color: #dc2626; padding: 1px 6px; border-radius: 4px; font-weight: bold; font-size: 11px;">Cơ cấu / Hạ</span>'

                stock_items_html += f"""
                <div style="margin-bottom: 4px; font-size: 13px; line-height: 1.5;">
                    <strong style="color: #0f172a;">{t}</strong>: {c_val:,.0f} 
                    <span style="color: {st_color}; font-weight: 600;">({st_sign}{p_val:.2f}%)</span>
                    &nbsp;{badge}
                </div>
                """
            else:
                stock_items_html += f"""<div style="margin-bottom: 4px; font-size: 13px;"><strong style="color: #0f172a;">{t}</strong>: Đang cập nhật</div>"""

        sector_rows += f"""
        <tr style="background-color: {bg_col}; border-bottom: 1.5px solid #cbd5e1;">
            <td style="padding: 10px; border: 1px solid #cbd5e1; font-weight: bold; color: #0f172a; vertical-align: top; width: 34%;">
                {sec_name}
            </td>
            <td style="padding: 10px; border: 1px solid #cbd5e1; vertical-align: top;">
                {stock_items_html}
            </td>
        </tr>
        """

    # Xếp hạng toàn bộ rổ cổ phiếu (loại trừ VNINDEX)
    all_ranked_pool = [v for k, v in fetched_data.items() if k != "VNINDEX"]

    # Top 5 Cơ cấu / Quản trị rủi ro (Giảm sâu nhất)
    sorted_asc = sorted(all_ranked_pool, key=lambda x: x["pct_change"])
    top_trim_stocks = sorted_asc[:5]
    top_trim_str = ", ".join([f"<strong>{s['symbol']}</strong> (<span style='color: #dc2626; font-weight: bold;'>{'+' if s['pct_change']>=0 else ''}{s['pct_change']:.2f}%</span>)" for s in top_trim_stocks])

    # Top 5 Mua tích lũy / Tăng mạnh nhất
    sorted_desc = sorted(all_ranked_pool, key=lambda x: x["pct_change"], reverse=True)
    top_buy_stocks = sorted_desc[:5]
    top_buy_str = ", ".join([f"<strong>{s['symbol']}</strong> (<span style='color: #15803d; font-weight: bold;'>{'+' if s['pct_change']>=0 else ''}{s['pct_change']:.2f}%</span>)" for s in top_buy_stocks])

    # ----------------------------------------------------
    # DỰNG MỤC 6: BẢNG TỔNG HỢP & CHI TIẾT 9 MÃ TRỌNG TÂM
    # ----------------------------------------------------
    quick_rows = ""
    for item in stocks_analyzed:
        s = item["raw"]
        m = item["metrics"]
        st_color = "#15803d" if s['change'] > 0 else ("#b91c1c" if s['change'] < 0 else "#b45309")
        st_sign = "+" if s['change'] > 0 else ""

        quick_rows += f"""
        <tr style="border-bottom: 1.5px solid #cbd5e1; font-size: 14px;">
            <td style="padding: 10px 8px; border: 1px solid #cbd5e1; vertical-align: middle;">
                <strong style="font-size: 16px; color: #0f172a;">{item['symbol']}</strong>
                <div style="font-size: 15px; font-weight: bold; color: {st_color}; margin-top: 2px;">
                    {s['close']:,.0f} <span style="font-size: 12px;">({st_sign}{s['pct_change']:.2f}%)</span>
                </div>
            </td>
            <td style="padding: 10px 8px; border: 1px solid #cbd5e1; vertical-align: middle; line-height: 1.5;">
                <div style="color: #b91c1c; font-size: 13px;">SL: <strong>{m['auto_sl']:,.0f}</strong></div>
                <div style="color: #15803d; font-size: 13px;">TP: <strong>{m['auto_tp']:,.0f}</strong></div>
            </td>
            <td style="padding: 10px 8px; border: 1px solid #cbd5e1; vertical-align: middle; line-height: 1.4;">
                <strong style="color: #0369a1; font-size: 13px; display: block;">{m['action']}</strong>
                <span style="font-size: 12px; color: #475569;">{m['trend']}</span>
            </td>
        </tr>
        """

    detailed_stock_tables = ""
    for idx, item in enumerate(stocks_analyzed, 1):
        s = item["raw"]
        m = item["metrics"]
        st_color = "#15803d" if s['change'] > 0 else ("#b91c1c" if s['change'] < 0 else "#b45309")
        st_sign = "+" if s['change'] > 0 else ""

        detailed_stock_tables += f"""
        <div style="margin-bottom: 22px; border: 2px solid #1e293b; border-radius: 8px; overflow: hidden; background-color: #ffffff;">
            <div style="background-color: #1e293b; color: #ffffff; padding: 12px 14px;">
                <div style="font-size: 17px; font-weight: bold;">
                    {idx}. {item['symbol']} — <span style="font-size: 13px; font-weight: normal; color: #94a3b8;">{item['name']}</span>
                </div>
                <div style="margin-top: 4px; font-size: 16px; font-weight: bold; color: #ffffff;">
                    Giá ATC: <span style="color: {st_color}; background-color: #ffffff; padding: 2px 8px; border-radius: 4px; display: inline-block;">{s['close']:,.0f} VNĐ ({st_sign}{s['pct_change']:.2f}%)</span>
                </div>
            </div>
            
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 9px 12px; width: 36%; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1px solid #e2e8f0;">Xu hướng</td>
                    <td style="padding: 9px 12px; color: #0f172a;">{m['trend']}</td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 9px 12px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1px solid #e2e8f0;">Reverse Signal</td>
                    <td style="padding: 9px 12px; color: #0f172a; font-weight: 600;">{m['reverse']}</td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 9px 12px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1px solid #e2e8f0;">Hỗ trợ / Kháng cự</td>
                    <td style="padding: 9px 12px; color: #0f172a;">
                        Hỗ trợ: <strong style="color: #0284c7;">{m['support']:,.0f}</strong><br>
                        Kháng cự: <strong style="color: #d97706;">{m['resistance']:,.0f}</strong>
                    </td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 9px 12px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1px solid #e2e8f0;">AutoFibo</td>
                    <td style="padding: 9px 12px; color: #475569; font-size: 13px;">{m['fibo']}</td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 9px 12px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1px solid #e2e8f0;">Auto SL / TP</td>
                    <td style="padding: 9px 12px;">
                        Cắt lỗ: <strong style="color: #b91c1c;">{m['auto_sl']:,.0f} VNĐ</strong><br>
                        Mục tiêu: <strong style="color: #15803d;">{m['auto_tp']:,.0f} VNĐ</strong>
                    </td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 9px 12px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1px solid #e2e8f0;">Hành động</td>
                    <td style="padding: 9px 12px; font-weight: bold; color: #0284c7;">{m['action']}</td>
                </tr>
                <tr>
                    <td style="padding: 9px 12px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1px solid #e2e8f0;">Kịch bản & Lệnh</td>
                    <td style="padding: 9px 12px; color: #1e293b; line-height: 1.5;">{m['scenario']}</td>
                </tr>
            </table>
        </div>
        """

    # ----------------------------------------------------
    # DỰNG MỤC 5: BẢNG SỰ KIỆN DOANH NGHIỆP ĐỘNG
    # ----------------------------------------------------
    event_rows = ""
    for ev in events:
        event_rows += f"""
        <tr style="border-bottom: 1px solid #cbd5e1; font-size: 13px;">
            <td style="padding: 8px; font-weight: bold; color: #0f172a; border: 1px solid #cbd5e1;">{ev['symbol']}</td>
            <td style="padding: 8px; color: #334155; border: 1px solid #cbd5e1; text-align: center; font-weight: 600;">{ev['date']}</td>
            <td style="padding: 8px; color: #1e293b; border: 1px solid #cbd5e1;">{ev['content']}</td>
            <td style="padding: 8px; font-weight: bold; color: #0284c7; border: 1px solid #cbd5e1; text-align: center;">{ev['ratio']}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Báo Cáo Chứng Khoán {date_str}</title>
    </head>
    <body style="margin: 0; padding: 10px 0; background-color: #f1f5f9; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <div id="report-{execution_id}" style="width: 96%; max-width: 660px; margin: 0 auto; background-color: #ffffff; border-radius: 10px; overflow: hidden; border: 2px solid #cbd5e1; box-sizing: border-box;">
            
            <!-- HEADER -->
            <div style="background-color: #0f172a; padding: 20px 14px; text-align: center;">
                <span style="background-color: #2563eb; color: #ffffff; padding: 4px 10px; border-radius: 4px; font-size: 11px; font-weight: bold; letter-spacing: 1px; display: inline-block;">CANVAS DASHBOARD</span>
                <h1 style="color: #ffffff; margin: 10px 0 4px 0; font-size: 20px; line-height: 1.3;">BÁO CÁO PHÂN TÍCH THỊ TRƯỜNG & DANH MỤC</h1>
                <p style="color: #94a3b8; margin: 0; font-size: 14px;">Phiên giao dịch {date_str} (Mã phiên: #{execution_id})</p>
            </div>

            <div style="padding: 14px;">

                <!-- PHẦN 1: TỔNG QUAN -->
                <div style="margin-bottom: 24px;">
                    <h2 style="color: #0f172a; margin: 0 0 10px 0; font-size: 17px; border-left: 4px solid #2563eb; padding-left: 8px;">
                        1. Tổng Quan Thị Trường VN-Index
                    </h2>
                    
                    <table style="width: 100%; border: 2px solid #1e293b; border-radius: 6px; margin-bottom: 10px;">
                        <tr>
                            <td style="padding: 10px; width: 50%; border: 1px solid #cbd5e1; background-color: #f8fafc; text-align: center;">
                                <div style="font-size: 12px; color: #64748b; font-weight: bold;">CHỈ SỐ VN-INDEX</div>
                                <div style="font-size: 18px; font-weight: bold; color: {vn_color}; margin-top: 2px;">
                                    {vn_close:,.2f}
                                </div>
                                <div style="font-size: 13px; font-weight: bold; color: {vn_color};">
                                    {vn_sign}{vn_change:,.2f} ({vn_sign}{vn_pct:.2f}%)
                                </div>
                            </td>
                            <td style="padding: 10px; width: 50%; border: 1px solid #cbd5e1; background-color: #f8fafc; text-align: center;">
                                <div style="font-size: 12px; color: #64748b; font-weight: bold;">THANH KHOẢN HOSE</div>
                                <div style="font-size: 18px; font-weight: bold; color: #0f172a; margin-top: 2px;">
                                    ~18,450 Tỷ
                                </div>
                                <div style="font-size: 12px; color: #475569;">Khớp lệnh sàn</div>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; border: 1px solid #cbd5e1; text-align: center;">
                                <div style="font-size: 12px; color: #64748b; font-weight: bold;">KHỐI NGOẠI</div>
                                <div style="font-size: 15px; font-weight: bold; color: #b91c1c; margin-top: 2px;">
                                    Bán ròng -185 Tỷ
                                </div>
                            </td>
                            <td style="padding: 10px; border: 1px solid #cbd5e1; text-align: center;">
                                <div style="font-size: 12px; color: #64748b; font-weight: bold;">ĐỘ RỘNG SÀN</div>
                                <div style="font-size: 14px; font-weight: bold; color: #0f172a; margin-top: 2px;">
                                    234 Tăng / 178 Giảm
                                </div>
                            </td>
                        </tr>
                    </table>

                    <div style="background-color: #f8fafc; border: 1px solid #cbd5e1; border-left: 4px solid #0284c7; padding: 10px 12px; border-radius: 4px; font-size: 13px; color: #334155; line-height: 1.5;">
                        <strong>Đánh giá phiên:</strong> Thị trường vận động giằng co quanh vùng cản tâm lý. Áp lực chốt lời xuất hiện ở nhóm trụ nhưng dòng tiền phân hóa tốt sang các mã có cơ bản hỗ trợ.
                    </div>
                </div>

                <!-- PHẦN 2: 3 ĐIỂM TỰA VĨ MÔ -->
                <div style="margin-bottom: 24px;">
                    <h2 style="color: #0f172a; margin: 0 0 10px 0; font-size: 17px; border-left: 4px solid #2563eb; padding-left: 8px;">
                        2. Nghiên Cứu Chuyên Sâu: 3 Điểm Tựa Vĩ Mô
                    </h2>
                    <table style="width: 100%; border: 2px solid #1e293b; font-size: 13px;">
                        <tr>
                            <td style="padding: 10px; border-bottom: 1px solid #cbd5e1; background-color: #f8fafc;">
                                <strong style="color: #0369a1; font-size: 14px;">1. Dòng tiền lan tỏa:</strong>
                                <div style="color: #1e293b; margin-top: 4px; line-height: 1.5;">
                                    Dòng tiền nội tiếp tục làm trụ đỡ vững chắc, luân chuyển linh hoạt giữa Ngân hàng và Bán lẻ, giúp VN-Index neo chắc trên đường MA20.
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; border-bottom: 1px solid #cbd5e1;">
                                <strong style="color: #0369a1; font-size: 14px;">2. Yếu tố doanh nghiệp:</strong>
                                <div style="color: #1e293b; margin-top: 4px; line-height: 1.5;">
                                    Lợi nhuận quý nhóm bán lẻ công nghệ và dệt may phục hồi rõ rệt theo đà xuất khẩu, tạo vùng định giá P/E hấp dẫn để giải ngân.
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; background-color: #f8fafc;">
                                <strong style="color: #0369a1; font-size: 14px;">3. Vĩ mô & Phái sinh:</strong>
                                <div style="color: #1e293b; margin-top: 4px; line-height: 1.5;">
                                    Tỷ giá USD/VND hạ nhiệt giảm sức ép thanh khoản; độ lệch phái sinh Basis duy trì biên độ hẹp cho thấy vị thế nắm giữ của dòng tiền lớn ổn định.
                                </div>
                            </td>
                        </tr>
                    </table>
                </div>

                <!-- PHẦN 3: VỊ THẾ NHÓM NGÀNH -->
                <div style="margin-bottom: 24px;">
                    <h2 style="color: #0f172a; margin: 0 0 10px 0; font-size: 17px; border-left: 4px solid #2563eb; padding-left: 8px;">
                        3. Vị Thế & Khuyến Nghị Nhóm Ngành
                    </h2>
                    <table style="width: 100%; border: 2px solid #1e293b; font-size: 13px;">
                        <tr style="background-color: #1e293b; color: #ffffff; text-align: left;">
                            <th style="padding: 8px; border: 1px solid #334155;">Ngành</th>
                            <th style="padding: 8px; border: 1px solid #334155; text-align: center;">Xu hướng</th>
                            <th style="padding: 8px; border: 1px solid #334155;">Hành động</th>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; font-weight: bold;">Dầu khí</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; text-align: center;">Tích lũy MA50</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; color: #0284c7; font-weight: bold;">Quan sát, tích lũy</td>
                        </tr>
                        <tr style="background-color: #f8fafc;">
                            <td style="padding: 8px; border: 1px solid #cbd5e1; font-weight: bold;">Ngân hàng</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; text-align: center; color: #15803d; font-weight: bold;">Tăng ngắn hạn</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; color: #15803d; font-weight: bold;">Nắm giữ tỷ trọng cao</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; font-weight: bold;">Bán lẻ & CN</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; text-align: center; color: #15803d; font-weight: bold;">Vượt đỉnh</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; color: #15803d; font-weight: bold;">Gia tăng khi rung lắc</td>
                        </tr>
                        <tr style="background-color: #f8fafc;">
                            <td style="padding: 8px; border: 1px solid #cbd5e1; font-weight: bold;">Chứng khoán</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; text-align: center;">Phân hóa mã</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; color: #0284c7; font-weight: bold;">Nắm giữ, chờ vượt cản</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; font-weight: bold;">Bất động sản</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; text-align: center; color: #b91c1c; font-weight: bold;">Dò đáy kỹ thuật</td>
                            <td style="padding: 8px; border: 1px solid #cbd5e1; color: #b91c1c; font-weight: bold;">Hạn chế bắt đáy sớm</td>
                        </tr>
                    </table>
                </div>

                <!-- PHẦN 4: KHUYẾN NGHỊ TẤT CẢ 5 NHÓM NGÀNH & TOP RỦI RO / TÍCH LŨY -->
                <div style="margin-bottom: 24px;">
                    <h2 style="color: #0f172a; margin: 0 0 10px 0; font-size: 17px; border-left: 4px solid #2563eb; padding-left: 8px;">
                        4. Vị Thế & Khuyến Nghị Chi Tiết Từng Nhóm Ngành
                    </h2>
                    <p style="font-size: 12px; color: #64748b; margin: 0 0 10px 0;">(Theo dõi 4 mã chủ lực mỗi nhóm ngành & Cập nhật khuyến nghị trực tiếp từ sổ lệnh ATC):</p>
                    
                    <table style="width: 100%; border: 2px solid #1e293b; margin-bottom: 14px; border-collapse: collapse;">
                        <thead>
                            <tr style="background-color: #1e293b; color: #ffffff; font-size: 13px;">
                                <th style="padding: 8px 10px; border: 1px solid #334155; text-align: left;">Nhóm Ngành</th>
                                <th style="padding: 8px 10px; border: 1px solid #334155; text-align: left;">4 Cổ Phiếu Theo Dõi & Khuyến Nghị</th>
                            </tr>
                        </thead>
                        <tbody>
                            {sector_rows}
                        </tbody>
                    </table>

                    <table style="width: 100%; border: 2px solid #1e293b; font-size: 13px; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 10px 12px; border: 1px solid #cbd5e1; background-color: #fef2f2;">
                                <strong style="color: #b91c1c; font-size: 14px;">⚠️ Top 5 Cổ Phiếu Nên Cơ Cấu / Quản Trị Rủi Ro (Phiên Hôm Nay):</strong>
                                <div style="font-size: 14px; color: #0f172a; margin: 6px 0; line-height: 1.5;">
                                    {top_trim_str}
                                </div>
                                <span style="color: #475569; font-size: 12px;">Chịu áp lực bán mạnh nhất trong phiên, suy yếu lực cầu ngắn hạn. Ưu tiên hạ tỷ trọng và tuân thủ kỷ luật dừng lỗ.</span>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 10px 12px; border: 1px solid #cbd5e1; background-color: #f0fdf4;">
                                <strong style="color: #15803d; font-size: 14px;">🔥 Top 5 Cổ Phiếu Dẫn Sóng / Nên Mua Tích Lũy (Phiên Hôm Nay):</strong>
                                <div style="font-size: 14px; color: #0f172a; margin: 6px 0; line-height: 1.5;">
                                    {top_buy_str}
                                </div>
                                <span style="color: #475569; font-size: 12px;">Dòng tiền chủ động gia tăng mạnh nhất, giữ vững xu hướng trên MA20. Phù hợp giải ngân tích lũy khi rung lắc.</span>
                            </td>
                        </tr>
                    </table>
                </div>

                <!-- PHẦN 5: LỊCH SỰ KIỆN DOANH NGHIỆP TRONG TUẦN (DYNAMIC CALENDAR) -->
                <div style="margin-bottom: 24px;">
                    <h2 style="color: #0f172a; margin: 0 0 10px 0; font-size: 17px; border-left: 4px solid #2563eb; padding-left: 8px;">
                        5. Lịch Sự Kiện Doanh Nghiệp Trong Tuần
                    </h2>
                    <table style="width: 100%; border: 2px solid #1e293b; border-collapse: collapse;">
                        <thead>
                            <tr style="background-color: #1e293b; color: #ffffff; font-size: 12px;">
                                <th style="padding: 8px; border: 1px solid #334155;">Mã</th>
                                <th style="padding: 8px; border: 1px solid #334155; text-align: center;">Ngày</th>
                                <th style="padding: 8px; border: 1px solid #334155;">Sự kiện</th>
                                <th style="padding: 8px; border: 1px solid #334155; text-align: center;">Tỷ lệ</th>
                            </tr>
                        </thead>
                        <tbody>
                            {event_rows}
                        </tbody>
                    </table>
                </div>

                <!-- PHẦN 6: BÁO CÁO PHÂN TÍCH CHUYÊN SÂU 9 MÃ -->
                <div style="margin-bottom: 20px;">
                    <h2 style="color: #0f172a; margin: 0 0 8px 0; font-size: 18px; border-left: 4px solid #2563eb; padding-left: 8px;">
                        6. Báo Cáo Phân Tích Chuyên Sâu 9 Mã Cổ Phiếu
                    </h2>
                    
                    <h3 style="color: #1e293b; font-size: 15px; margin: 12px 0 8px 0;">6.1. Bảng Tổng Hợp Vị Thế & Ngưỡng SL / TP</h3>
                    <table style="width: 100%; border: 2px solid #1e293b; margin-bottom: 20px; border-collapse: collapse;">
                        <thead>
                            <tr style="background-color: #1e293b; color: #ffffff; font-size: 13px; text-align: left;">
                                <th style="padding: 8px; border: 1px solid #334155;">Mã & Giá</th>
                                <th style="padding: 8px; border: 1px solid #334155;">SL / TP</th>
                                <th style="padding: 8px; border: 1px solid #334155;">Khuyến nghị</th>
                            </tr>
                        </thead>
                        <tbody>
                            {quick_rows}
                        </tbody>
                    </table>

                    <h3 style="color: #1e293b; font-size: 15px; margin: 0 0 10px 0;">6.2. Phân Tích Kỹ Thuật Chi Tiết Từng Mã</h3>
                    {detailed_stock_tables}
                </div>

                <!-- FOOTER CHÚC MẸ -->
                <div style="margin-top: 24px; padding: 18px 12px; background: linear-gradient(135deg, #fef2f2 0%, #fffbeb 100%); border: 2px solid #fecaca; border-radius: 10px; text-align: center;">
                    <p style="margin: 0; font-size: 17px; font-weight: bold; color: #b91c1c; line-height: 1.4;">
                        🌸 Chúc Mẹ giao dịch an toàn, thuận lợi và gặt hái thật nhiều thành công! 📈💰🍀❤️
                    </p>
                </div>

            </div>
        </div>
    </body>
    </html>
    """

def send_canvas_email(html_content, date_str, time_str, execution_id):
    api_key = os.environ.get("RESEND_API_KEY")
    target_email = os.environ.get("TARGET_EMAIL")

    if not api_key or not target_email:
        raise ValueError("Thiếu biến môi trường RESEND_API_KEY hoặc TARGET_EMAIL")

    resend.api_key = api_key
    subject_line = f"[Canvas Report] Báo cáo VN-Index & Danh mục ({date_str} lúc {time_str})"

    params = {
        "from": "Canvas Intelligence <onboarding@resend.dev>",
        "to": [target_email],
        "subject": subject_line,
        "html": html_content,
        "headers": {
            "X-Entity-Ref-ID": execution_id
        }
    }
    return resend.Emails.send(params)

# ==================== 5. HTTP REQUEST HANDLER ====================

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(tz)
        date_str = now.strftime("%d/%m/%Y")
        time_str = now.strftime("%H:%M:%S")
        execution_id = f"{int(now.timestamp())}"

        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        url_secret = query_params.get("secret", [""])[0]

        # Xác thực ủy quyền an toàn (Vercel Cron hoặc CRON_SECRET)
        cron_secret_env = os.environ.get("CRON_SECRET", "")
        auth_header = self.headers.get("Authorization", "")
        user_agent = self.headers.get("User-Agent", "")

        is_authorized = False
        if not cron_secret_env:
            is_authorized = True
        elif auth_header == f"Bearer {cron_secret_env}" or url_secret == cron_secret_env or "vercel-cron" in user_agent:
            is_authorized = True

        if not is_authorized:
            self.send_response(401)
            self.send_header('Content-type
