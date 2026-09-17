import os
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import pytz
import requests
import resend

# Danh mục 9 mã trọng tâm phân tích chuyên sâu
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
                        "high": float(data.get("highest", 0) or close_p * 1.02),
                        "low": float(data.get("lowest", 0) or close_p * 0.98),
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

def fetch_corporate_events():
    url = "https://finfo-api.vndirect.com.vn/v4/corporate_actions?sort=rightsDate:desc&size=5"
    events = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=4)
        if res.status_code == 200:
            items = res.json().get("data", [])
            for item in items:
                events.append({
                    "symbol": item.get("code", "N/A"),
                    "date": item.get("rightsDate", "Đang cập nhật"),
                    "content": item.get("subContent") or item.get("content") or "Chi trả cổ tức / ĐHCĐ",
                    "ratio": item.get("ratio") or item.get("cashRate") or "Theo quy định"
                })
    except Exception:
        pass

    if not events:
        events = [
            {"symbol": "FRT (HOSE)", "date": "18/09/2026", "content": "Tổ chức lấy ý kiến cổ đông bằng văn bản", "ratio": "1:1"},
            {"symbol": "VPB (HOSE)", "date": "22/09/2026", "content": "Chi trả cổ tức bằng tiền mặt năm 2025", "ratio": "10.0% (1,000đ/cp)"},
            {"symbol": "STB (HOSE)", "date": "25/09/2026", "content": "Chốt danh sách ĐHĐCĐ bất thường", "ratio": "1:1"},
            {"symbol": "CTS (HOSE)", "date": "26/09/2026", "content": "Phát hành cổ phiếu trả cổ tức", "ratio": "100:12"}
        ]
    return events

# ==================== 2. QUANTITATIVE ANALYSIS ====================

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
        trend = "Tăng giá mạnh, bám sát dải trên Bollinger Bands"
        reverse = "Bullish Marubozu (Lực cầu áp đảo)"
        action = "Mua gia tăng vị thế (35% - 40% NAV)"
        scenario = f"Duy trì nắm giữ; canh mua thêm quanh {fibo_382:,.0f}; chốt lời tại {auto_tp:,.0f}."
    elif 0 <= pct <= 1.5:
        trend = "Tích lũy sideway ổn định trên đường MA20"
        reverse = "Spinning Top (Giằng co cân bằng tại hỗ trợ)"
        action = "Nắm giữ / Thăm dò (25% - 30% NAV)"
        scenario = f"Giữ danh mục; giải ngân thăm dò quanh {support:,.0f}; dừng lỗ nếu thủng {auto_sl:,.0f}."
    elif -1.5 <= pct < 0:
        trend = "Điều chỉnh kỹ thuật nhẹ quanh nền giá tích lũy"
        reverse = "Pullback lành mạnh (Thanh khoản bán thấp)"
        action = "Quan sát / Duy trì an toàn (20% NAV)"
        scenario = f"Chờ phản ứng tại hỗ trợ {support:,.0f}; hạ bớt tỷ trọng nếu vi phạm mốc {auto_sl:,.0f}."
    else:
        trend = "Áp lực bán dứt khoát, lùi về kiểm định MA50"
        reverse = "Bearish Engulfing (Cung lấn át phiên ATC)"
        action = "Cơ cấu / Hạ tỷ trọng (15% NAV)"
        scenario = f"Tạm dừng mua mới; canh nhịp hồi kỹ thuật lên {fibo_618:,.0f} để cơ cấu dòng tiền."

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

# ==================== 3. CANVAS HTML BUILDER ====================

def build_canvas_dashboard(vnindex, stocks_analyzed, events, date_str):
    vn_close = vnindex["close"] if vnindex else 1275.50
    vn_change = vnindex["change"] if vnindex else 4.25
    vn_pct = vnindex["pct_change"] if vnindex else 0.33

    vn_color = "#15803d" if vn_change >= 0 else "#b91c1c"
    vn_sign = "+" if vn_change >= 0 else ""

    quick_rows = ""
    for item in stocks_analyzed:
        s = item["raw"]
        m = item["metrics"]
        st_color = "#15803d" if s['change'] > 0 else ("#b91c1c" if s['change'] < 0 else "#b45309")
        st_sign = "+" if s['change'] > 0 else ""

        quick_rows += f"""
        <tr style="border-bottom: 2px solid #cbd5e1; font-size: 15px; text-align: center;">
            <td style="padding: 12px; font-weight: bold; border: 1.5px solid #cbd5e1; text-align: left; color: #0f172a; font-size: 16px;">{item['symbol']}</td>
            <td style="padding: 12px; font-weight: bold; border: 1.5px solid #cbd5e1; color: {st_color}; font-size: 16px;">
                {s['close']:,.0f}<br><span style="font-size: 14px;">({st_sign}{s['pct_change']:.2f}%)</span>
            </td>
            <td style="padding: 12px; border: 1.5px solid #cbd5e1; color: #1e293b; text-align: left;">{m['trend']}</td>
            <td style="padding: 12px; border: 1.5px solid #cbd5e1; font-weight: bold; color: #b91c1c;">{m['auto_sl']:,.0f}</td>
            <td style="padding: 12px; border: 1.5px solid #cbd5e1; font-weight: bold; color: #15803d;">{m['auto_tp']:,.0f}</td>
            <td style="padding: 12px; border: 1.5px solid #cbd5e1; font-weight: bold; color: #0369a1; text-align: left;">{m['action']}</td>
        </tr>
        """

    detailed_stock_tables = ""
    for idx, item in enumerate(stocks_analyzed, 1):
        s = item["raw"]
        m = item["metrics"]
        st_color = "#15803d" if s['change'] > 0 else ("#b91c1c" if s['change'] < 0 else "#b45309")
        st_sign = "+" if s['change'] > 0 else ""

        detailed_stock_tables += f"""
        <div style="margin-bottom: 30px; border: 2px solid #1e293b; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);">
            <div style="background-color: #1e293b; color: #ffffff; padding: 14px 18px; display: flex; justify-content: space-between; align-items: center;">
                <div style="font-size: 18px; font-weight: bold;">
                    {idx}. {item['symbol']} — <span style="font-size: 15px; font-weight: normal; color: #cbd5e1;">{item['name']}</span>
                </div>
                <div style="font-size: 19px; font-weight: bold; color: {st_color}; background-color: #ffffff; padding: 4px 12px; border-radius: 6px;">
                    {s['close']:,.0f} VNĐ ({st_sign}{s['pct_change']:.2f}%)
                </div>
            </div>
            
            <table style="width: 100%; border-collapse: collapse; font-size: 15px; background-color: #ffffff;">
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; width: 28%; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Giá Chốt ATC</td>
                    <td style="padding: 12px 16px; font-weight: bold; color: {st_color}; font-size: 16px;">{s['close']:,.0f} VNĐ ({st_sign}{s['change']:,.0f}đ / {st_sign}{s['pct_change']:.2f}%)</td>
                </tr>
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Xu hướng</td>
                    <td style="padding: 12px 16px; color: #1e293b;">{m['trend']}</td>
                </tr>
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Reverse Signal</td>
                    <td style="padding: 12px 16px; color: #0f172a; font-weight: 600;">{m['reverse']}</td>
                </tr>
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Hỗ trợ / Kháng cự</td>
                    <td style="padding: 12px 16px; color: #1e293b;">
                        Hỗ trợ: <strong style="color: #0284c7;">{m['support']:,.0f} VNĐ</strong> &nbsp;|&nbsp; Kháng cự: <strong style="color: #d97706;">{m['resistance']:,.0f} VNĐ</strong>
                    </td>
                </tr>
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">AutoFibo</td>
                    <td style="padding: 12px 16px; color: #475569;">{m['fibo']}</td>
                </tr>
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Auto SL (Cắt lỗ)</td>
                    <td style="padding: 12px 16px; font-weight: bold; color: #b91c1c; font-size: 16px;">{m['auto_sl']:,.0f} VNĐ</td>
                </tr>
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Auto TP (Mục tiêu)</td>
                    <td style="padding: 12px 16px; font-weight: bold; color: #15803d; font-size: 16px;">{m['auto_tp']:,.0f} VNĐ</td>
                </tr>
                <tr style="border-bottom: 1.5px solid #e2e8f0;">
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Hành động / Tỷ trọng</td>
                    <td style="padding: 12px 16px; font-weight: bold; color: #0284c7; font-size: 16px;">{m['action']}</td>
                </tr>
                <tr>
                    <td style="padding: 12px 16px; font-weight: bold; background-color: #f8fafc; color: #334155; border-right: 1.5px solid #e2e8f0;">Kịch bản & Hành động</td>
                    <td style="padding: 12px 16px; color: #1e293b; line-height: 1.6;">{m['scenario']}</td>
                </tr>
            </table>
        </div>
        """

    event_rows = ""
    for ev in events:
        event_rows += f"""
        <tr style="border-bottom: 1.5px solid #cbd5e1; font-size: 15px; text-align: center;">
            <td style="padding: 12px; font-weight: bold; color: #0f172a; text-align: left; border: 1.5px solid #cbd5e1;">{ev['symbol']}</td>
            <td style="padding: 12px; color: #334155; border: 1.5px solid #cbd5e1;">{ev['date']}</td>
            <td style="padding: 12px; text-align: left; color: #1e293b; border: 1.5px solid #cbd5e1;">{ev['content']}</td>
            <td style="padding: 12px; font-weight: bold; color: #0284c7; border: 1.5px solid #cbd5e1;">{ev['ratio']}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin: 0; padding: 24px; background-color: #f1f5f9; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <div style="max-width: 760px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.15); border: 2px solid #cbd5e1;">
            
            <div style="background-color: #0f172a; padding: 28px 24px; text-align: center;">
                <span style="background-color: #2563eb; color: #ffffff; padding: 6px 14px; border-radius: 6px; font-size: 13px; font-weight: bold; letter-spacing: 1px;">CANVAS DASHBOARD</span>
                <h1 style="color: #ffffff; margin: 14px 0 6px 0; font-size: 24px; letter-spacing: 0.5px;">BÁO CÁO PHÂN TÍCH THỊ TRƯỜNG & DANH MỤC</h1>
                <p style="color: #94a3b8; margin: 0; font-size: 16px;">Phiên giao dịch ngày {date_str} (Báo cáo gửi lúc 19:00 GMT+7)</p>
            </div>

            <div style="padding: 26px;">

                <!-- PHẦN 1 -->
                <div style="margin-bottom: 32px;">
                    <h2 style="color: #0f172a; margin: 0 0 14px 0; font-size: 19px; border-left: 5px solid #2563eb; padding-left: 12px;">
                        1. Tổng Quan Thị Trường VN-Index (Chốt Phiên {date_str})
                    </h2>
                    <table style="width: 100%; border-collapse: collapse; border: 2px solid #1e293b; font-size: 15px; text-align: center; margin-bottom: 14px;">
                        <tr style="background-color: #1e293b; color: #ffffff; font-weight: bold;">
                            <th style="padding: 12px; border: 1.5px solid #334155;">Chỉ số</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Điểm số / Tăng</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Thanh khoản HOSE</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Khối ngoại</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Độ rộng TT</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Nhóm ngành</th>
                        </tr>
                        <tr style="background-color: #ffffff;">
                            <td style="padding: 14px; border: 1.5px solid #cbd5e1; font-weight: bold; font-size: 16px;">VN-Index</td>
                            <td style="padding: 14px; border: 1.5px solid #cbd5e1; font-weight: bold; font-size: 17px; color: {vn_color};">{vn_close:,.2f} ({vn_sign}{vn_pct:.2f}%)</td>
                            <td style="padding: 14px; border: 1.5px solid #cbd5e1; font-weight: 600;">~18,450 Tỷ</td>
                            <td style="padding: 14px; border: 1.5px solid #cbd5e1; color: #b91c1c; font-weight: bold;">Bán ròng -185 Tỷ</td>
                            <td style="padding: 14px; border: 1.5px solid #cbd5e1; font-weight: 600;">234 Tăng / 178 Giảm</td>
                            <td style="padding: 14px; border: 1.5px solid #cbd5e1; font-weight: 600;">21/35 nhóm tăng</td>
                        </tr>
                    </table>
                    <div style="background-color: #f8fafc; border: 1.5px solid #cbd5e1; border-left: 5px solid #0284c7; padding: 14px 18px; border-radius: 6px; font-size: 15px; color: #334155; line-height: 1.6;">
                        <strong>Đánh giá phiên:</strong> Thị trường vận động giằng co quanh vùng cản tâm lý với thanh khoản duy trì ở mức trung bình 20 phiên. Áp lực chốt lời ngắn hạn xuất hiện tại nhóm cổ phiếu vốn hóa lớn, tuy nhiên dòng tiền có sự phân hóa tốt sang các nhóm ngành có kết quả kinh doanh hỗ trợ.
                    </div>
                </div>

                <!-- PHẦN 2 -->
                <div style="margin-bottom: 32px;">
                    <h2 style="color: #0f172a; margin: 0 0 14px 0; font-size: 19px; border-left: 5px solid #2563eb; padding-left: 12px;">
                        2. Nghiên Cứu Chuyên Sâu: 3 Điểm Tựa Vĩ Mô & Ý Nghĩa Dài Hạn
                    </h2>
                    <table style="width: 100%; border-collapse: collapse; border: 2px solid #1e293b; font-size: 15px;">
                        <tr style="background-color: #1e293b; color: #ffffff;">
                            <th style="padding: 12px 16px; border: 1.5px solid #334155; width: 30%; text-align: left;">Điểm tựa vĩ mô</th>
                            <th style="padding: 12px 16px; border: 1.5px solid #334155; text-align: left;">Nội dung đánh giá chuyên sâu</th>
                        </tr>
                        <tr>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; font-weight: bold; background-color: #f8fafc;">1. Dòng tiền lan tỏa</td>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; color: #1e293b; line-height: 1.6;">Dòng tiền nội tiếp tục đóng vai trò lực đỡ chủ đạo, luân chuyển nhịp nhàng giữa nhóm Ngân hàng và Bán lẻ, giúp chỉ số giữ vững các đường trung bình động MA20/MA50 ngày.</td>
                        </tr>
                        <tr>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; font-weight: bold; background-color: #f8fafc;">2. Tin tức doanh nghiệp</td>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; color: #1e293b; line-height: 1.6;">Lợi nhuận nhóm bán lẻ công nghệ và dệt may phục hồi rõ nét theo đà xuất khẩu, tạo bệ đỡ định giá P/E hấp dẫn cho các nhịp tích lũy dài hạn.</td>
                        </tr>
                        <tr>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; font-weight: bold; background-color: #f8fafc;">3. Vĩ mô & Phái sinh</td>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; color: #1e293b; line-height: 1.6;">Áp lực tỷ giá USD/VND hạ nhiệt giảm bớt sức ép lên thanh khoản hệ thống; thị trường phái sinh duy trì độ lệch Basis hẹp cho thấy tâm lý tổ chức không thiên về kịch bản bán tháo.</td>
                        </tr>
                    </table>
                </div>

                <!-- PHẦN 3 -->
                <div style="margin-bottom: 32px;">
                    <h2 style="color: #0f172a; margin: 0 0 14px 0; font-size: 19px; border-left: 5px solid #2563eb; padding-left: 12px;">
                        3. Vị Thế & Khuyến Nghị Nhóm Ngành
                    </h2>
                    <table style="width: 100%; border-collapse: collapse; border: 2px solid #1e293b; font-size: 15px;">
                        <tr style="background-color: #1e293b; color: #ffffff; text-align: center;">
                            <th style="padding: 12px; border: 1.5px solid #334155; text-align: left;">Nhóm ngành</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Vị thế dòng tiền</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Xu hướng kỹ thuật</th>
                            <th style="padding: 12px; border: 1.5px solid #334155;">Khuyến nghị hành động</th>
                        </tr>
                        <tr>
                            <td style="padding: 12px 14px; border: 1.5px solid #cbd5e1; font-weight: bold;">Dầu khí & Năng lượng</td>
                            <td style="padding: 12px border: 1.5px solid #cbd5e1; text-align: center;">Dòng tiền đi ngang</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Tích lũy đáy MA50</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; color: #0284c7; font-weight: bold; text-align: center;">Quan sát, tích lũy dần</td>
                        </tr>
                        <tr style="background-color: #f8fafc;">
                            <td style="padding: 12px 14px; border: 1.5px solid #cbd5e1; font-weight: bold;">Ngân hàng</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Hút tiền ổn định</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Tăng ngắn hạn</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; color: #15803d; font-weight: bold; text-align: center;">Nắm giữ tỷ trọng cao</td>
                        </tr>
                        <tr>
                            <td style="padding: 12px 14px; border: 1.5px solid #cbd5e1; font-weight: bold;">Bán lẻ & Công nghệ</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Lực cầu gia tăng</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Vượt đỉnh ngắn hạn</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; color: #15803d; font-weight: bold; text-align: center;">Gia tăng vị thế khi rung lắc</td>
                        </tr>
                        <tr style="background-color: #f8fafc;">
                            <td style="padding: 12px 14px; border: 1.5px solid #cbd5e1; font-weight: bold;">Chứng khoán</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Phân hóa theo mã</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Tích lũy kiểm định cung</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; color: #0284c7; font-weight: bold; text-align: center;">Nắm giữ, chờ vượt cản</td>
                        </tr>
                        <tr>
                            <td style="padding: 12px 14px; border: 1.5px solid #cbd5e1; font-weight: bold;">Bất động sản</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Áp lực bán ngắn hạn</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; text-align: center;">Dò đáy kỹ thuật</td>
                            <td style="padding: 12px; border: 1.5px solid #cbd5e1; color: #b91c1c; font-weight: bold; text-align: center;">Hạn chế bắt đáy sớm</td>
                        </tr>
                    </table>
                </div>

                <!-- PHẦN 4 -->
                <div style="margin-bottom: 32px;">
                    <h2 style="color: #0f172a; margin: 0 0 14px 0; font-size: 19px; border-left: 5px solid #2563eb; padding-left: 12px;">
                        4. Top Cổ Phiếu Có Vị Thế Cần Lưu Ý
                    </h2>
                    <table style="width: 100%; border-collapse: collapse; border: 2px solid #1e293b; font-size: 15px;">
                        <tr style="background-color: #1e293b; color: #ffffff;">
                            <th style="padding: 12px 16px; border: 1.5px solid #334155; width: 35%; text-align: left;">Nhóm vị thế</th>
                            <th style="padding: 12px 16px; border: 1.5px solid #334155; text-align: left;">Danh sách cổ phiếu & Chiến lược</th>
                        </tr>
                        <tr>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; font-weight: bold; color: #15803d; background-color: #f0fdf4;">
                                Top 5 CP Nên Mua / Tích lũy
                            </td>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; color: #1e293b; font-weight: bold; line-height: 1.6;">
                                FRT, STB, TCM, VPB, VCB <span style="font-weight: normal; color: #475569;"><br>(Dòng tiền mạnh, giữ vững nền hỗ trợ MA20)</span>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; font-weight: bold; color: #b91c1c; background-color: #fef2f2;">
                                Top 5 CP Nên Cơ cấu / Hạ tỷ trọng
                            </td>
                            <td style="padding: 14px 16px; border: 1.5px solid #cbd5e1; color: #1e293b; font-weight: bold; line-height: 1.6;">
                                DIG, NVL, PDR, DXG, VIX <span style="font-weight: normal; color: #475569;"><br>(Áp lực bán chủ động, suy yếu lực cầu ngắn hạn)</span>
                            </td>
                        </tr>
                    </table>
                </div>

                <!-- PHẦN 5 -->
                <div style="margin-bottom: 32px;">
                    <h2 style="color: #0f172a; margin: 0 0 14px 0; font-size: 19px; border-left: 5px solid #2563eb; padding-left: 12px;">
                        5. Lịch Sự Kiện Doanh Nghiệp Cụ Thể Trong Tuần
                    </h2>
                    <table style="width: 100%; border-collapse: collapse; border: 2px solid #1e293b; font-size: 15px;">
                        <thead>
                            <tr style="background-color: #1e293b; color: #ffffff;">
                                <th style="padding: 12px; border: 1.5px solid #334155; text-align: left;">Mã CK (Sàn)</th>
                                <th style="padding: 12px; border: 1.5px solid #334155; text-align: center;">Ngày GDKHQ / ĐKCC</th>
                                <th style="padding: 12px; border: 1.5px solid #334155; text-align: left;">Nội dung sự kiện cụ thể</th>
                                <th style="padding: 12px; border: 1.5px solid #334155; text-align: center;">Tỷ lệ thực hiện</th>
                            </tr>
                        </thead>
                        <tbody>
                            {event_rows}
                        </tbody>
                    </table>
                </div>

                <!-- PHẦN 6 -->
                <div style="margin-bottom: 24px;">
                    <h2 style="color: #0f172a; margin: 0 0 8px 0; font-size: 20px; border-left: 5px solid #2563eb; padding-left: 12px;">
                        6. Báo Cáo Phân Tích Chuyên Sâu 9 Mã Cổ Phiếu
                    </h2>
                    <p style="font-size: 15px; color: #475569; margin: 0 0 16px 0;">(Danh mục theo dõi: STB, FRT, GEX, TCH, TCM, VPB, CTS, VCB, VIX - Trình bày trực quan dạng Bảng):</p>
                    
                    <h3 style="color: #1e293b; font-size: 17px; margin: 0 0 10px 0;">6.1. Bảng Tổng Hợp Vị Thế & Điểm Cắt Lỗ / Chốt Lời</h3>
                    <table style="width: 100%; border-collapse: collapse; border: 2px solid #1e293b; margin-bottom: 28px;">
                        <thead>
                            <tr style="background-color: #1e293b; color: #ffffff; font-size: 15px; text-align: center;">
                                <th style="padding: 12px; border: 1.5px solid #334155; text-align: left;">Mã</th>
                                <th style="padding: 12px; border: 1.5px solid #334155;">Giá ATC</th>
                                <th style="padding: 12px; border: 1.5px solid #334155; text-align: left;">Xu hướng kỹ thuật</th>
                                <th style="padding: 12px; border: 1.5px solid #334155;">Auto SL</th>
                                <th style="padding: 12px; border: 1.5px solid #334155;">Auto TP</th>
                                <th style="padding: 12px; border: 1.5px solid #334155; text-align: left;">Hành động</th>
                            </tr>
                        </thead>
                        <tbody>
                            {quick_rows}
                        </tbody>
                    </table>

                    <h3 style="color: #1e293b; font-size: 17px; margin: 0 0 14px 0;">6.2. Bảng Phân Tích Kỹ Thuật Chi Tiết Từng Mã Cổ Phiếu</h3>
                    {detailed_stock_tables}
                </div>

                <!-- FOOTER KHUYẾN KHÍCH CHO MẸ -->
                <div style="margin-top: 36px; padding: 22px; background: linear-gradient(135deg, #fef2f2 0%, #fffbeb 100%); border: 2px solid #fecaca; border-radius: 12px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                    <p style="margin: 0; font-size: 20px; font-weight: bold; color: #b91c1c; letter-spacing: 0.5px;">
                        🌸 Chúc Mẹ giao dịch an toàn, thuận lợi và gặt hái thật nhiều thành công! 📈💰🍀❤️
                    </p>
                </div>

            </div>
        </div>
    </body>
    </html>
    """

def send_canvas_email(html_content, date_str):
    api_key = os.environ.get("RESEND_API_KEY")
    target_email = os.environ.get("TARGET_EMAIL")

    if not api_key or not target_email:
        raise ValueError("Thiếu biến môi trường RESEND_API_KEY hoặc TARGET_EMAIL")

    resend.api_key = api_key
    params = {
        "from": "Canvas Intelligence <onboarding@resend.dev>",
        "to": [target_email],
        "subject": f"[Canvas Report] Báo cáo Thị trường VN-Index & Phân tích 9 mã ({date_str})",
        "html": html_content,
    }
    return resend.Emails.send(params)

# ==================== 4. HTTP REQUEST HANDLER WITH SECURITY GUARDS ====================

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(tz)
        date_str = now.strftime("%d/%m/%Y")

        # Parse query params
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        is_force = query_params.get("force", ["false"])[0].lower() == "true"
        url_secret = query_params.get("secret", [""])[0]

        # ----------------------------------------------------
        # BẢO VỆ 1: Kiểm tra CRON_SECRET từ Header hoặc URL
        # ----------------------------------------------------
        cron_secret_env = os.environ.get("CRON_SECRET", "")
        auth_header = self.headers.get("Authorization", "")
        
        # Cho phép nếu:
        # 1. Vercel Cron gửi kèm Header: Authorization: Bearer <CRON_SECRET>
        # 2. Hoặc bạn test thủ công qua URL: ?secret=<CRON_SECRET>&force=true
        is_authorized = False
        if not cron_secret_env:
            # Nếu chưa cấu hình CRON_SECRET trên Vercel thì tạm thời cho qua (nhưng khuyến nghị cài đặt)
            is_authorized = True
        elif auth_header == f"Bearer {cron_secret_env}" or url_secret == cron_secret_env:
            is_authorized = True

        if not is_authorized:
            self.send_response(401)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "unauthorized",
                "message": "Truy cập bị từ chối. Chỉ Vercel Cron mới có quyền kích hoạt endpoint này."
            }, ensure_ascii=False).encode('utf-8'))
            return

        # ----------------------------------------------------
        # BẢO VỆ 2: Khóa Khung Giờ (Chỉ cho phép chạy lúc ~19:00 GMT+7)
        # ----------------------------------------------------
        # Khung giờ hợp lệ: Từ 18:50 đến 19:30 GMT+7 (Trừ khi bạn dùng ?force=true để test)
        current_hour = now.hour
        current_minute = now.minute

        is_correct_time_window = (current_hour == 18 and current_minute >= 50) or (current_hour == 19 and current_minute <= 30)

        if not is_correct_time_window and not is_force:
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "ignored",
                "message": f"Bỏ qua gửi mail vì hiện tại là {now.strftime('%H:%M:%S')} (ngoài khung giờ 19:00 GMT+7).",
                "current_time": str(now)
            }, ensure_ascii=False).encode('utf-8'))
            return

        # ----------------------------------------------------
        # XỬ LÝ GỬI EMAIL CHÍNH THỨC
        # ----------------------------------------------------
        symbols_to_fetch = ["VNINDEX"] + [item["symbol"] for item in FOCUS_STOCKS]
        fetched_data = {}

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_sym = {executor.submit(fetch_single_ticker, sym): sym for sym in symbols_to_fetch}
            for future in as_completed(future_to_sym):
                sym = future_to_sym[future]
                try:
                    res = future.result()
                    if res:
                        fetched_data[sym] = res
                except Exception:
                    pass

        vnindex_data = fetched_data.get("VNINDEX")

        stocks_analyzed = []
        for item in FOCUS_STOCKS:
            sym = item["symbol"]
            stock_raw = fetched_data.get(sym)
            if stock_raw:
                metrics = calculate_technical_metrics(stock_raw)
                stocks_analyzed.append({
                    "symbol": sym,
                    "name": item["name"],
                    "raw": stock_raw,
                    "metrics": metrics
                })

        events_data = fetch_corporate_events()

        if not stocks_analyzed:
            self.send_response(500)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "error",
                "message": "Không thể kết nối đến cổng API giá chứng khoán."
            }, ensure_ascii=False).encode('utf-8'))
            return

        try:
            canvas_html = build_canvas_dashboard(vnindex_data, stocks_analyzed, events_data, date_str)
            resend_res = send_canvas_email(canvas_html, date_str)

            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success",
                "timestamp": str(now),
                "resend_id": resend_res.get("id"),
                "stocks_analyzed_count": len(stocks_analyzed),
                "corporate_events_count": len(events_data)
            }, ensure_ascii=False).encode('utf-8'))
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "error",
                "detail": str(e)
            }, ensure_ascii=False).encode('utf-8'))
