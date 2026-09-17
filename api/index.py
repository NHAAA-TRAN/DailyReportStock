import os
import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler
import pytz
import requests
import resend

# Cấu hình danh sách mã cần lấy dữ liệu
WATCHLIST = ["TCM", "TCH", "CTS"]

def get_stock_data_dnse(symbol: str):
    """
    Truy vấn trực tiếp REST API từ DNSE Entrade Data Feed.
    Lấy giá khớp lệnh phiên mới nhất (sau ATC).
    """
    url = f"https://services.entrade.com.vn/chart-api/v2/ohlc/stock?resolution=1D&symbol={symbol}"
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        res.raise_for_status()
        data = res.json()
        
        # 'c': Close prices, 'v': Volume, 't': Timestamps
        if not data.get('c') or len(data['c']) < 2:
            return None
        
        close_price = data['c'][-1]
        prev_close = data['c'][-2]
        change = close_price - prev_close
        pct_change = (change / prev_close) * 100 if prev_close else 0
        volume = data['v'][-1]
        
        return {
            "symbol": symbol,
            "close": close_price,
            "change": change,
            "pct_change": pct_change,
            "volume": volume
        }
    except Exception as e:
        print(f"[ERROR] Truy xuất {symbol} thất bại: {str(e)}")
        return None

def build_html_table(stocks_data, date_str):
    rows = ""
    for item in stocks_data:
        # Phân loại màu chuẩn bảng điện: Xanh (>0), Đỏ (<0), Vàng (tham chiếu)
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
            <td style="padding: 12px; font-weight: bold; font-size: 14px; text-align: left; color: #1e293b;">{item['symbol']}</td>
            <td style="padding: 12px; font-weight: bold; font-size: 15px; color: {color};">{item['close']:,.0f}</td>
            <td style="padding: 12px; font-weight: 600; color: {color};">{sign}{item['change']:,.0f} ({sign}{item['pct_change']:.2f}%)</td>
            <td style="padding: 12px; color: #64748b;">{item['volume']:,}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin: 0; padding: 20px; background-color: #f8fafc; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <div style="max-width: 580px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #e2e8f0;">
            <div style="background-color: #0f172a; padding: 20px; text-align: center;">
                <h2 style="color: #ffffff; margin: 0; font-size: 18px; letter-spacing: 0.5px;">BÁO CÁO GIÁ ĐÓNG CỬA ATC</h2>
                <p style="color: #94a3b8; margin: 6px 0 0 0; font-size: 13px;">Ngày: {date_str} (Dữ liệu chốt lúc 18:00 GMT+7)</p>
            </div>
            
            <div style="padding: 20px;">
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr style="background-color: #f1f5f9; color: #475569; font-size: 12px; text-transform: uppercase;">
                            <th style="padding: 10px; text-align: left;">Mã CP</th>
                            <th style="padding: 10px;">Giá Đóng Cửa</th>
                            <th style="padding: 10px;">Thay Đổi</th>
                            <th style="padding: 10px;">Khối Lượng</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
                <div style="margin-top: 20px; padding: 12px; background-color: #f8fafc; border-left: 3px solid #3b82f6; font-size: 12px; color: #64748b; line-height: 1.5;">
                    <strong>Nguyên tắc hệ thống:</strong> Dữ liệu được trích xuất trực tiếp từ sổ lệnh sàn HOSE/HNX, loại bỏ hoàn toàn suy đoán giá và thiên kiến định tính.
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
        "subject": f"[Stock Report] Dữ liệu chốt phiên {date_str} - TCM, TCH, CTS",
        "html": html_content,
    }

    # Gửi qua API của Resend
    response = resend.Emails.send(params)
    return response

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(tz)
        date_str = now.strftime("%d/%m/%Y")

        stocks_data = []
        for symbol in WATCHLIST:
            data = get_stock_data_dnse(symbol)
            if data:
                stocks_data.append(data)

        # Kiểm tra tính toàn vẹn: nếu thiếu bất kỳ mã nào, ngừng flow để tránh gửi sai
        if len(stocks_data) != len(WATCHLIST):
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "error",
                "message": "Không đủ dữ liệu cho toàn bộ watchlist"
            }).encode('utf-8'))
            return

        try:
            html = build_html_table(stocks_data, date_str)
            resend_res = send_email_resend(html, date_str)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success",
                "timestamp": str(now),
                "resend_id": resend_res.get("id"),
                "symbols_count": len(stocks_data)
            }).encode('utf-8'))
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "error",
                "detail": str(e)
            }).encode('utf-8'))
