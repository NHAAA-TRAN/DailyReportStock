import os
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
import pytz
import requests
import resend

# Danh sách 9 mã theo dõi
WATCHLIST = ["TCM", "TCH", "CTS", "FRT", "GEX", "STB", "VCB", "VIX", "VPB"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

def get_from_ssi(symbol: str):
    """Nguồn 1: SSI iBoard API (Tốc độ cao, không chặn Vercel IP)"""
    url = f"https://iboard-query.ssi.com.vn/stock/stockDetail?stockSymbol={symbol}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=3)
        if res.status_code == 200:
            data = res.json().get("data", {})
            if data and data.get("stockSymbol"):
                close_p = float(data.get("matchedPrice", 0) or data.get("closePrice", 0))
                ref_p = float(data.get("refPrice", 0))

                # Chuẩn hóa về đơn vị đồng
                if 0 < close_p < 1000:
                    close_p *= 1000
                if 0 < ref_p < 1000:
                    ref_p *= 1000

                change = close_p - ref_p if ref_p else 0
                pct_change = (change / ref_p) * 100 if ref_p else 0
                volume = int(data.get("totalMatchVolume", 0) or data.get("nmVolume", 0))

                if close_p > 0:
                    return {
                        "symbol": symbol,
                        "close": close_p,
                        "change": change,
                        "pct_change": pct_change,
                        "volume": volume,
                        "source": "SSI"
                    }
    except Exception:
        pass
    return None

def get_from_dnse(symbol: str):
    """Nguồn 2: DNSE Entrade API"""
    url = f"https://services.entrade.com.vn/chart-api/v2/ohlc/stock?resolution=1D&symbol={symbol}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=3)
        if res.status_code == 200:
            data = res.json()
            if data.get('c') and len(data['c']) >= 2:
                close_p = float(data['c'][-1])
                prev_p = float(data['c'][-2])
                change = close_p - prev_p
                pct_change = (change / prev_p) * 100 if prev_p else 0
                volume = int(data['v'][-1]) if data.get('v') else 0

                return {
                    "symbol": symbol,
                    "close": close_p,
                    "change": change,
                    "pct_change": pct_change,
                    "volume": volume,
                    "source": "DNSE"
                }
    except Exception:
        pass
    return None

def get_from_vndirect_dchart(symbol: str):
    """Nguồn 3: VNDIRECT TradingView UDF (Cổng mở toàn cầu)"""
    now_ts = int(time.time())
    from_ts = now_ts - (15 * 86400)
    url = f"https://dchart-api.vndirect.com.vn/dchart/history?resolution=D&symbol={symbol}&from={from_ts}&to={now_ts}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=3)
        if res.status_code == 200:
            data = res.json()
            if data.get('s') == 'ok' and data.get('c') and len(data['c']) >= 2:
                raw_c = float(data['c'][-1])
                raw_prev = float(data['c'][-2])
                close_p = raw_c * 1000 if raw_c < 1000 else raw_c
                prev_p = raw_prev * 1000 if raw_prev < 1000 else raw_prev
                change = close_p - prev_p
                pct_change = (change / prev_p) * 100 if prev_p else 0
                volume = int(data['v'][-1]) if data.get('v') else 0

                return {
                    "symbol": symbol,
                    "close": close_p,
                    "change": change,
                    "pct_change": pct_change,
                    "volume": volume,
                    "source": "VND_DCHART"
                }
    except Exception:
        pass
    return None

def fetch_stock_price(symbol: str):
    """Thử lần lượt SSI -> DNSE -> VNDIRECT Dchart"""
    data = get_from_ssi(symbol)
    if data:
        return data

    data = get_from_dnse(symbol)
    if data:
        return data

    data = get_from_vndirect_dchart(symbol)
    if data:
        return data

    return None

def fetch_all_watchlist_parallel(symbols):
    """Chạy song song tất cả các mã qua ThreadPoolExecutor để tối ưu tốc độ"""
    results = {}
    with ThreadPoolExecutor(max_workers=9) as executor:
        future_to_symbol = {executor.submit(fetch_stock_price, sym): sym for sym in symbols}
        for future in as_completed(future_to_symbol):
            sym = future_to_symbol[future]
            try:
                res = future.result()
                if res:
                    results[sym] = res
            except Exception:
                pass
    
    # Đảm bảo giữ đúng thứ tự hiển thị như WATCHLIST
    ordered_results = [results[s] for s in symbols if s in results]
    failed = [s for s in symbols if s not in results]
    return ordered_results, failed

def build_html_table(stocks_data, failed_symbols, date_str):
    rows = ""
    for item in stocks_data:
        if item['change'] > 0:
            color = "#16a34a"
            sign = "+"
        elif item['change'] < 0:
            color = "#dc2626"
            sign = ""
        else:
            color = "#d97706"
            sign = ""

        rows += f"""
        <tr style="border-bottom: 1px solid #e2e8f0; text-align: center;">
            <td style="padding: 10px 12px; font-weight: bold; font-size: 14px; text-align: left; color: #1e293b;">{item['symbol']}</td>
            <td style="padding: 10px 12px; font-weight: bold; font-size: 15px; color: {color};">{item['close']:,.0f}</td>
            <td style="padding: 10px 12px; font-weight: 600; color: {color};">{sign}{item['change']:,.0f} ({sign}{item['pct_change']:.2f}%)</td>
            <td style="padding: 10px 12px; color: #64748b; font-size: 13px;">{item['volume']:,}</td>
        </tr>
        """

    failed_alert = ""
    if failed_symbols:
        failed_alert = f"""
        <div style="margin-top: 15px; padding: 10px; background-color: #fef2f2; border: 1px solid #fecaca; border-radius: 6px; font-size: 12px; color: #b91c1c;">
            <strong>Mã tạm thời chưa lấy được:</strong> {', '.join(failed_symbols)}
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin: 0; padding: 20px; background-color: #f8fafc; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #e2e8f0;">
            <div style="background-color: #0f172a; padding: 20px; text-align: center;">
                <h2 style="color: #ffffff; margin: 0; font-size: 18px; letter-spacing: 0.5px;">BÁO CÁO GIÁ ĐÓNG CỬA ATC</h2>
                <p style="color: #94a3b8; margin: 6px 0 0 0; font-size: 13px;">Ngày: {date_str} (Dữ liệu chốt phiên)</p>
            </div>
            
            <div style="padding: 20px;">
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr style="background-color: #f1f5f9; color: #475569; font-size: 12px; text-transform: uppercase;">
                            <th style="padding: 10px 12px; text-align: left;">Mã CP</th>
                            <th style="padding: 10px 12px;">Giá Đóng</th>
                            <th style="padding: 10px 12px;">Thay Đổi</th>
                            <th style="padding: 10px 12px;">Khối Lượng</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
                {failed_alert}
                <div style="margin-top: 20px; padding: 12px; background-color: #f8fafc; border-left: 3px solid #3b82f6; font-size: 12px; color: #64748b; line-height: 1.5;">
                    <strong>Nguyên tắc hệ thống:</strong> Dữ liệu được trích xuất trực tiếp từ cổng API bảng giá chứng khoán sau phiên ATC, không qua suy đoán hoặc tổng hợp định tính của LLM.
                </div>
            </div>
        </div>
    </body>
    </html>
    """

def send_email_resend(html_content, date_str):
    api_key = os.environ.get("RESEND_API_KEY")
    target_email = os.environ.get("TARGET_EMAIL")

    if not api_key or not target_email:
        raise ValueError("Thiếu biến môi trường RESEND_API_KEY hoặc TARGET_EMAIL")

    resend.api_key = api_key

    params = {
        "from": "Stock Assistant <onboarding@resend.dev>",
        "to": [target_email],
        "subject": f"[Stock Report] Báo cáo chốt phiên {date_str} ({len(WATCHLIST)} mã)",
        "html": html_content,
    }

    return resend.Emails.send(params)

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(tz)
        date_str = now.strftime("%d/%m/%Y")

        stocks_data, failed_symbols = fetch_all_watchlist_parallel(WATCHLIST)

        if not stocks_data:
            self.send_response(500)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            err_msg = json.dumps({
                "status": "error",
                "message": "Không thể kết nối đến các cổng API chứng khoán",
                "failed_symbols": failed_symbols
            }, ensure_ascii=False)
            self.wfile.write(err_msg.encode('utf-8'))
            return

        try:
            html = build_html_table(stocks_data, failed_symbols, date_str)
            resend_res = send_email_resend(html, date_str)

            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            success_msg = json.dumps({
                "status": "success",
                "timestamp": str(now),
                "resend_id": resend_res.get("id"),
                "total_requested": len(WATCHLIST),
                "successful_count": len(stocks_data),
                "failed_symbols": failed_symbols
            }, ensure_ascii=False)
            self.wfile.write(success_msg.encode('utf-8'))
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            err_msg = json.dumps({
                "status": "error",
                "detail": str(e)
            }, ensure_ascii=False)
            self.wfile.write(err_msg.encode('utf-8'))
