#!/usr/bin/env python3
"""
fetch_data.py — Tải dữ liệu HOSE THẬT và xuất ra data.js cho dashboard
======================================================================
Dùng thư viện vnstock kéo dữ liệu OHLCV (giá mở/cao/thấp/đóng + khối lượng)
theo ngày cho VN-Index và danh sách mã, rồi ghi ra file `data.js`:

    window.HOSE_DATA = { asof, universe, index, stocks };

Dashboard (HOSE_Signal_Cockpit.html) sẽ TỰ ĐỘNG đọc file này nếu nằm cùng
thư mục: có data.js -> hiện "Dữ liệu thật"; không có -> chạy chế độ minh hoạ.

Chạy thủ công:   python fetch_data.py
Tự động hằng ngày: dùng GitHub Actions (xem file .github/workflows/update-daily.yml)
Yêu cầu:         pip install -U vnstock pandas
"""
from __future__ import annotations
import json, sys, datetime as dt

START = "2014-01-01"
SOURCE = "VCI"            # 'VCI' | 'TCBS' | 'KBS' (KBS ổn định nhất trên server)
OUT_JS = "data.js"
OUT_JSON = "data.json"    # bản JSON tuỳ chọn (cho công cụ khác)

# Danh sách mã theo dõi: mã -> (tên hiển thị, ngành)
UNIVERSE = {
    "VCB": ("Vietcombank", "Ngân hàng"),
    "VIC": ("Vingroup", "Bất động sản"),
    "VHM": ("Vinhomes", "Bất động sản"),
    "HPG": ("Hòa Phát", "Thép"),
    "FPT": ("FPT", "Công nghệ"),
    "SSI": ("SSI", "Chứng khoán"),
    "MWG": ("Thế Giới Di Động", "Bán lẻ"),
    "GAS": ("PV Gas", "Dầu khí"),
    "STB": ("Sacombank", "Ngân hàng"),
    "VNM": ("Vinamilk", "Tiêu dùng"),
    # Muốn thêm mã: chỉ cần thêm một dòng "MÃ": ("Tên", "Ngành"),
}


def _normalize(df):
    """Chuẩn hóa cột về open/high/low/close/volume, index = ngày."""
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    tcol = next((c for c in ("time", "date", "tradingdate") if c in df.columns), None)
    if tcol:
        df[tcol] = df[tcol].astype(str).str.slice(0, 10)
        df = df.set_index(tcol)
    keep = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
    df = df[keep].dropna()
    return df


def _series(df):
    """Đổi DataFrame thành dict các mảng song song; tự nhân 1000 nếu giá ở đơn vị nghìn."""
    closes = df["close"].astype(float)
    scale = 1000.0 if closes.median() < 1000 else 1.0  # vnstock đôi khi trả giá nghìn đồng
    return {
        "dates": list(df.index.astype(str)),
        "open": [round(v * scale) for v in df["open"].astype(float)],
        "high": [round(v * scale) for v in df["high"].astype(float)],
        "low":  [round(v * scale) for v in df["low"].astype(float)],
        "close": [round(v * scale) for v in closes],
        "volume": [int(v) for v in df["volume"].astype(float)],
    }


def fetch_one(symbol, start, end, source):
    from vnstock import Vnstock
    stock = Vnstock().stock(symbol=symbol, source=source)
    raw = stock.quote.history(start=start, end=end, interval="1D")
    return _normalize(raw)


def fetch_index(start, end):
    """Lấy VN-Index. Thử vài cách vì tên/nguồn có thể khác nhau giữa các phiên bản."""
    from vnstock import Vnstock
    for sym, src in [("VNINDEX", "VCI"), ("VNINDEX", "TCBS"), ("VNINDEX", "KBS")]:
        try:
            stock = Vnstock().stock(symbol=sym, source=src)
            df = _normalize(stock.quote.history(start=start, end=end, interval="1D"))
            if not df.empty:
                return df
        except Exception:
            continue
    return None


def main():
    end = dt.date.today().isoformat()
    print(f"Tải dữ liệu HOSE {START} → {end} (nguồn {SOURCE})...\n")

    data = {"asof": end, "universe": {}, "index": None, "stocks": {}}

    # VN-Index
    try:
        idx = fetch_index(START, end)
        if idx is not None and not idx.empty:
            data["index"] = {"dates": list(idx.index.astype(str)),
                             "close": [round(float(v), 2) for v in idx["close"].astype(float)]}
            print(f"  [VN-Index] {len(idx)} phiên  ✔")
        else:
            print("  [VN-Index] không lấy được — dashboard sẽ dùng chỉ số minh hoạ ở tab Sự kiện.")
    except Exception as exc:
        print(f"  [VN-Index] lỗi: {exc}")

    # Các mã
    ok = 0
    for sym, (name, sector) in UNIVERSE.items():
        try:
            df = fetch_one(sym, START, end, SOURCE)
            if df.empty or len(df) < 60:
                print(f"  [{sym}] không đủ dữ liệu, bỏ qua.")
                continue
            data["stocks"][sym] = _series(df)
            data["universe"][sym] = {"name": name, "sector": sector}
            ok += 1
            print(f"  [{sym}] {len(df)} phiên  ✔")
        except Exception as exc:
            print(f"  [{sym}] lỗi: {exc}")

    if ok == 0:
        print("\nKhông tải được mã nào. Kiểm tra mạng và 'pip install -U vnstock'.")
        sys.exit(1)

    # Ghi data.js (dashboard đọc trực tiếp) + data.json (tuỳ chọn)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    with open(OUT_JS, "w", encoding="utf-8") as f:
        f.write("window.HOSE_DATA = " + payload + ";\n")
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        f.write(payload)

    size_mb = len(payload) / 1e6
    print(f"\nĐã ghi {OUT_JS} ({size_mb:.2f} MB) — {ok} mã, cập nhật tới {end}.")
    print("Đặt data.js cùng thư mục với HOSE_Signal_Cockpit.html rồi mở dashboard.")


if __name__ == "__main__":
    main()
