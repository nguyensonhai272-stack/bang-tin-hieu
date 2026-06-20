#!/usr/bin/env python3
"""
fetch_data.py — Tải dữ liệu HOSE thật, xuất data.js cho dashboard
Dùng vnstock 4.x API mới + tự chờ khi bị rate limit
"""
import json, sys, time, datetime as dt

START = "2014-01-01"
SOURCE = "VCI"
OUT_JS = "data.js"
OUT_JSON = "data.json"

UNIVERSE = {
    "VCB": ("Vietcombank",        "Ngân hàng"),
    "VIC": ("Vingroup",           "Bất động sản"),
    "VHM": ("Vinhomes",           "Bất động sản"),
    "HPG": ("Hòa Phát",           "Thép"),
    "FPT": ("FPT",                "Công nghệ"),
    "SSI": ("SSI",                "Chứng khoán"),
    "MWG": ("Thế Giới Di Động",   "Bán lẻ"),
    "GAS": ("PV Gas",             "Dầu khí"),
    "STB": ("Sacombank",          "Ngân hàng"),
    "VNM": ("Vinamilk",           "Tiêu dùng"),
}

DELAY = 65   # giây chờ sau mỗi 18 request (an toàn dưới giới hạn 20/phút)
REQ_PER_BATCH = 18


def _normalize(df):
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    tcol = next((c for c in ("time", "date", "tradingdate") if c in df.columns), None)
    if tcol:
        df[tcol] = df[tcol].astype(str).str.slice(0, 10)
        df = df.set_index(tcol)
    keep = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
    return df[keep].dropna()


def _series(df):
    closes = df["close"].astype(float)
    scale = 1000.0 if closes.median() < 1000 else 1.0
    return {
        "dates":  list(df.index.astype(str)),
        "open":   [round(v * scale) for v in df["open"].astype(float)],
        "high":   [round(v * scale) for v in df["high"].astype(float)],
        "low":    [round(v * scale) for v in df["low"].astype(float)],
        "close":  [round(v * scale) for v in closes],
        "volume": [int(v) for v in df["volume"].astype(float)],
    }


def fetch_with_retry(symbol, start, end, source, max_retries=4):
    """Tải dữ liệu 1 mã, tự chờ và thử lại khi bị rate limit."""
    from vnstock.api.quote import Quote
    for attempt in range(max_retries):
        try:
            q = Quote(symbol=symbol, source=source)
            raw = q.history(start=start, end=end, interval="1D")
            df = _normalize(raw)
            if not df.empty:
                return df
            raise ValueError("Dữ liệu rỗng")
        except Exception as e:
            msg = str(e).lower()
            if "rate limit" in msg or "429" in msg or "giới hạn" in msg:
                wait = DELAY * (attempt + 1)
                print(f"    ⏳ Rate limit — chờ {wait}s rồi thử lại ({attempt+1}/{max_retries})...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Không tải được {symbol} sau {max_retries} lần thử.")


def fetch_index(start, end):
    """Tải VN-Index."""
    for sym in ["VNINDEX", "VNI"]:
        try:
            df = fetch_with_retry(sym, start, end, SOURCE)
            if not df.empty:
                return df
        except Exception:
            continue
    return None


def main():
    end = dt.date.today().isoformat()
    print(f"Tải dữ liệu HOSE {START} → {end} | nguồn {SOURCE}\n")

    data = {"asof": end, "universe": {}, "index": None, "stocks": {}}
    req_count = 0

    # VN-Index
    try:
        print("  [VN-Index]", end=" ", flush=True)
        idx = fetch_index(START, end)
        req_count += 1
        if idx is not None and not idx.empty:
            data["index"] = {
                "dates": list(idx.index.astype(str)),
                "close": [round(float(v), 2) for v in idx["close"].astype(float)]
            }
            print(f"{len(idx)} phiên ✔")
        else:
            print("không lấy được, bỏ qua.")
    except Exception as e:
        print(f"lỗi: {e}")

    # Các mã — chờ sau mỗi batch để tránh rate limit
    ok = 0
    for i, (sym, (name, sector)) in enumerate(UNIVERSE.items()):
        # Chờ sau mỗi REQ_PER_BATCH request
        if req_count > 0 and req_count % REQ_PER_BATCH == 0:
            print(f"\n  ⏳ Đã gửi {req_count} request — chờ {DELAY}s để tránh rate limit...")
            time.sleep(DELAY)

        print(f"  [{sym}]", end=" ", flush=True)
        try:
            df = fetch_with_retry(sym, START, end, SOURCE)
            req_count += 1
            if len(df) < 60:
                print("không đủ dữ liệu, bỏ qua.")
                continue
            data["stocks"][sym] = _series(df)
            data["universe"][sym] = {"name": name, "sector": sector}
            ok += 1
            print(f"{len(df)} phiên ✔")
        except Exception as e:
            print(f"lỗi: {e}")
        
        # Thêm khoảng nghỉ nhỏ giữa các mã để không hit burst limit
        time.sleep(4)

    if ok == 0:
        print("\n❌ Không tải được mã nào. Kiểm tra mạng hoặc đổi SOURCE='TCBS'.")
        sys.exit(1)

    # Ghi file
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    with open(OUT_JS,   "w", encoding="utf-8") as f:
        f.write("window.HOSE_DATA = " + payload + ";\n")
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        f.write(payload)

    mb = len(payload) / 1e6
    print(f"\n✅ Xong: {ok} mã | {mb:.2f} MB | cập nhật tới {end}")

if __name__ == "__main__":
    main()
