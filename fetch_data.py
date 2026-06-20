#!/usr/bin/env python3
"""
fetch_data.py v2 — Tải dữ liệu HOSE: giá OHLCV + chỉ số cơ bản
Dùng vnstock 4.x API mới + tự chờ khi bị rate limit
"""
import json, sys, time, datetime as dt

START = "2014-01-01"
SOURCE = "VCI"
OUT_JS   = "data.js"
OUT_JSON = "data.json"

UNIVERSE = {
    "VCB": ("Vietcombank",       "Ngân hàng"),
    "VIC": ("Vingroup",          "Bất động sản"),
    "VHM": ("Vinhomes",          "Bất động sản"),
    "HPG": ("Hòa Phát",          "Thép"),
    "FPT": ("FPT",               "Công nghệ"),
    "SSI": ("SSI",               "Chứng khoán"),
    "MWG": ("Thế Giới Di Động",  "Bán lẻ"),
    "GAS": ("PV Gas",            "Dầu khí"),
    "STB": ("Sacombank",         "Ngân hàng"),
    "VNM": ("Vinamilk",          "Tiêu dùng"),
    "ACB": ("ACB",               "Ngân hàng"),
    "MBB": ("MB Bank",           "Ngân hàng"),
    "TCB": ("Techcombank",       "Ngân hàng"),
    "BID": ("BIDV",              "Ngân hàng"),
    "CTG": ("VietinBank",        "Ngân hàng"),
}

DELAY          = 65   # giây chờ giữa các batch
REQ_PER_BATCH  = 15   # request mỗi batch (dưới giới hạn 20/phút)
INTER_DELAY    = 4    # giây nghỉ giữa các mã


def _normalize(df):
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    tcol = next((c for c in ("time","date","tradingdate") if c in df.columns), None)
    if tcol:
        df[tcol] = df[tcol].astype(str).str.slice(0, 10)
        df = df.set_index(tcol)
    keep = [c for c in ("open","high","low","close","volume") if c in df.columns]
    return df[keep].dropna()


def _series(df):
    closes = df["close"].astype(float)
    scale  = 1000.0 if closes.median() < 1000 else 1.0
    return {
        "dates":  list(df.index.astype(str)),
        "open":   [round(v * scale) for v in df["open"].astype(float)],
        "high":   [round(v * scale) for v in df["high"].astype(float)],
        "low":    [round(v * scale) for v in df["low"].astype(float)],
        "close":  [round(v * scale) for v in closes],
        "volume": [int(v) for v in df["volume"].astype(float)],
    }


def fetch_with_retry(symbol, start, end, source, max_retries=4):
    from vnstock.api.quote import Quote
    for attempt in range(max_retries):
        try:
            q   = Quote(symbol=symbol, source=source)
            raw = q.history(start=start, end=end, interval="1D")
            df  = _normalize(raw)
            if not df.empty:
                return df
            raise ValueError("Dữ liệu rỗng")
        except Exception as e:
            msg = str(e).lower()
            if "rate limit" in msg or "429" in msg or "giới hạn" in msg:
                wait = DELAY * (attempt + 1)
                print(f"    ⏳ Rate limit — chờ {wait}s ({attempt+1}/{max_retries})...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Không tải được {symbol} sau {max_retries} lần thử.")


def fetch_index(start, end):
    for sym in ["VNINDEX", "VNI"]:
        try:
            df = fetch_with_retry(sym, start, end, SOURCE)
            if not df.empty:
                return df
        except Exception:
            continue
    return None


def fetch_fundamental(symbol):
    """
    Lấy chỉ số cơ bản: P/E, P/B, EPS, ROE, ROA, tăng trưởng EPS.
    Trả về dict hoặc {} nếu lỗi.
    """
    try:
        # vnstock 4.x: Finance / Valuation
        from vnstock.api.valuation import Valuation
        v   = Valuation(symbol=symbol, source=SOURCE)
        df  = v.ratio()            # bảng tỷ số định giá
        if df is None or df.empty:
            return {}
        df.columns = [str(c).lower() for c in df.columns]
        last = df.iloc[-1]
        def g(keys, default=None):
            for k in keys:
                if k in last.index and last[k] is not None:
                    try: return float(last[k])
                    except: pass
            return default
        return {
            "pe":          g(["p/e","pe","price_to_earning"]),
            "pb":          g(["p/b","pb","price_to_book"]),
            "eps":         g(["eps","earningpershare"]),
            "roe":         g(["roe","return_on_equity"]),
            "roa":         g(["roa","return_on_asset"]),
            "eps_growth":  g(["eps_growth","epsgrowth","eps_yoy"]),
            "revenue_growth": g(["revenue_growth","revenuegrowth","revenue_yoy"]),
        }
    except Exception:
        pass
    # fallback: thử Finance API
    try:
        from vnstock.api.financial import Finance
        f  = Finance(symbol=symbol, source=SOURCE)
        df = f.ratio(period="year", lang="en")
        if df is None or df.empty:
            return {}
        df.columns = [str(c).lower() for c in df.columns]
        last = df.iloc[-1]
        def g2(keys, default=None):
            for k in keys:
                if k in last.index and last[k] is not None:
                    try: return float(last[k])
                    except: pass
            return default
        return {
            "pe":             g2(["p/e","pe"]),
            "pb":             g2(["p/b","pb"]),
            "eps":            g2(["eps"]),
            "roe":            g2(["roe"]),
            "roa":            g2(["roa"]),
            "eps_growth":     g2(["eps_growth","epsgrowth"]),
            "revenue_growth": g2(["revenue_growth","revenuegrowth"]),
        }
    except Exception:
        return {}


def main():
    end = dt.date.today().isoformat()
    print(f"Tải dữ liệu HOSE {START} → {end} | nguồn {SOURCE}\n")

    data = {"asof": end, "universe": {}, "index": None,
            "stocks": {}, "fundamentals": {}}
    req_count = 0

    # ── VN-Index ──────────────────────────────────────────────
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
            print("không lấy được.")
    except Exception as e:
        print(f"lỗi: {e}")

    # ── Giá OHLCV từng mã ─────────────────────────────────────
    print("\n── Tải giá OHLCV ──")
    ok_price = 0
    for sym, (name, sector) in UNIVERSE.items():
        if req_count > 0 and req_count % REQ_PER_BATCH == 0:
            print(f"\n  ⏳ {req_count} request — chờ {DELAY}s...")
            time.sleep(DELAY)

        print(f"  [{sym}]", end=" ", flush=True)
        try:
            df = fetch_with_retry(sym, START, end, SOURCE)
            req_count += 1
            if len(df) < 60:
                print("không đủ dữ liệu.")
                continue
            data["stocks"][sym]   = _series(df)
            data["universe"][sym] = {"name": name, "sector": sector}
            ok_price += 1
            print(f"{len(df)} phiên ✔")
        except Exception as e:
            print(f"lỗi: {e}")

        time.sleep(INTER_DELAY)

    # ── Chỉ số cơ bản ─────────────────────────────────────────
    print("\n── Tải chỉ số cơ bản (P/E, P/B, ROE…) ──")
    ok_fund = 0
    for sym in list(data["universe"].keys()):
        if req_count > 0 and req_count % REQ_PER_BATCH == 0:
            print(f"\n  ⏳ {req_count} request — chờ {DELAY}s...")
            time.sleep(DELAY)

        print(f"  [{sym}]", end=" ", flush=True)
        try:
            fund = fetch_fundamental(sym)
            req_count += 1
            data["fundamentals"][sym] = fund
            if fund:
                pe  = f"P/E {fund['pe']:.1f}" if fund.get("pe") else ""
                roe = f"ROE {fund['roe']*100:.1f}%" if fund.get("roe") else ""
                print(f"{pe} {roe} ✔")
                ok_fund += 1
            else:
                print("không có dữ liệu cơ bản.")
        except Exception as e:
            print(f"lỗi: {e}")
            data["fundamentals"][sym] = {}

        time.sleep(INTER_DELAY)

    if ok_price == 0:
        print("\n❌ Không tải được mã nào. Kiểm tra mạng hoặc đổi SOURCE='TCBS'.")
        sys.exit(1)

    # ── Ghi file ──────────────────────────────────────────────
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    with open(OUT_JS,   "w", encoding="utf-8") as f:
        f.write("window.HOSE_DATA = " + payload + ";\n")
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        f.write(payload)

    mb = len(payload) / 1e6
    print(f"\n✅ Xong: {ok_price} mã giá | {ok_fund} mã cơ bản | {mb:.2f} MB | {end}")

if __name__ == "__main__":
    main()
