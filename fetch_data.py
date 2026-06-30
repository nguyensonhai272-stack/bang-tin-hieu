#!/usr/bin/env python3
"""
fetch_data.py v4 — VN100 + lọc thanh khoản 50 tỷ/phiên
=========================================================
Pipeline:
  1. Tải danh sách VN100 từ vnstock (fallback: danh sách cứng)
  2. Tải OHLCV từng mã (vnstock 4.x API), chờ tránh rate limit
  3. Lọc mã: gtgd TB 20 phiên ≥ LIQ_MIN_BILLION tỷ đồng
  4. Tính tín hiệu kỹ thuật ngay trên server (không cần trình duyệt tính)
  5. Xuất signals.js (~nhỏ, bảng tín hiệu) + data.js (OHLCV đầy đủ cho tab chi tiết)
"""
import json, sys, time, datetime as dt, math
import numpy as np, pandas as pd

SOURCE          = "VCI"
START           = "2018-01-01"
LIQ_MIN_BILLION = 50          # tỷ đồng/phiên TB 20 ngày
OUT_SIGNALS     = "signals.js"
OUT_DATA        = "data.js"
OUT_JSON        = "data.json"

# URL CSV của Google Sheet tin tức vĩ mô (do Gemini cập nhật mỗi sáng).
# Hoạt động sau khi bạn chia sẻ Sheet ở chế độ "Bất kỳ ai có đường liên kết → Người xem".
NEWS_CSV_URL    = "https://docs.google.com/spreadsheets/d/1u8UsZ5kj0TGzmBAD3nkNW2DOIiX84ROxAVz4qxfK1hw/export?format=csv&gid=0"

DELAY          = 65
REQ_PER_BATCH  = 15
INTER_DELAY    = 3

# ── Danh sách VN100 cứng (fallback khi API listing không trả được) ──────────
VN100_FALLBACK = [
    # Ngân hàng
    "VCB","BID","CTG","MBB","TCB","ACB","STB","VPB","HDB","VIB",
    "MSB","LPB","SHB","OCB","TPB","SSB","ABB","BAB","BVB","KLB",
    # Bất động sản
    "VIC","VHM","NVL","PDR","DXG","KDH","BCM","VRE","IJC","NLG",
    "HDC","DIG","CII","SJS","AGG","VPI","NBB","HBC","LDG","TDC",
    # Chứng khoán
    "SSI","VND","HCM","MBS","CTS","BSI","FTS","APG","ART","TVB",
    # Thép & Vật liệu
    "HPG","HSG","NKG","TLH","VGS","SMC","POM","TVN","DTL","VIS",
    # Công nghệ & Viễn thông
    "FPT","CMG","ELC","SGT","VGI","ITD","SAM","SED","TST","VTC",
    # Dầu khí
    "GAS","PLX","PVD","PVS","BSR","OIL","PVC","CNG","PGD","PSH",
    # Tiêu dùng & Bán lẻ
    "VNM","MWG","MSN","SAB","BHN","KDC","MCH","QNS","SBT","LSS",
    # Hàng không & Vận tải
    "HVN","VJC","ACV","GMD","PVT","HAH","VOS","VSC","SFI","TCO",
    # Năng lượng & Điện
    "POW","NT2","GEG","REE","PPC","VSH","SHP","CHP","TMP","TBC",
    # Bảo hiểm & Đầu tư
    "BVH","PVI","BMI","ABI","MIG","PTI","VNR","BIC","PRE","PGI",
    # Mã mới thêm 06/2026 (VN30 mới + theo yêu cầu)
    "VPL","GVR","DGC","EIB","PNJ","GEX","GEL","GEE",
]

SECTOR_MAP = {
    "VCB":"Ngân hàng","BID":"Ngân hàng","CTG":"Ngân hàng","MBB":"Ngân hàng",
    "TCB":"Ngân hàng","ACB":"Ngân hàng","STB":"Ngân hàng","VPB":"Ngân hàng",
    "HDB":"Ngân hàng","VIB":"Ngân hàng","MSB":"Ngân hàng","LPB":"Ngân hàng",
    "SHB":"Ngân hàng","OCB":"Ngân hàng","TPB":"Ngân hàng","SSB":"Ngân hàng",
    "VIC":"Bất động sản","VHM":"Bất động sản","NVL":"Bất động sản",
    "PDR":"Bất động sản","DXG":"Bất động sản","KDH":"Bất động sản",
    "BCM":"Bất động sản","VRE":"Bất động sản","DIG":"Bất động sản",
    "NLG":"Bất động sản","HDC":"Bất động sản","CII":"Bất động sản",
    "SSI":"Chứng khoán","VND":"Chứng khoán","HCM":"Chứng khoán",
    "MBS":"Chứng khoán","CTS":"Chứng khoán","BSI":"Chứng khoán",
    "FTS":"Chứng khoán","VCI":"Chứng khoán",
    "HPG":"Thép & Vật liệu","HSG":"Thép & Vật liệu","NKG":"Thép & Vật liệu",
    "TLH":"Thép & Vật liệu","SMC":"Thép & Vật liệu",
    "FPT":"Công nghệ","CMG":"Công nghệ","ELC":"Công nghệ",
    "GAS":"Dầu khí","PLX":"Dầu khí","PVD":"Dầu khí","PVS":"Dầu khí",
    "BSR":"Dầu khí","OIL":"Dầu khí",
    "VNM":"Tiêu dùng","MWG":"Bán lẻ","MSN":"Tiêu dùng","SAB":"Tiêu dùng",
    "MCH":"Tiêu dùng","KDC":"Tiêu dùng",
    "HVN":"Hàng không","VJC":"Hàng không","ACV":"Hàng không",
    "GMD":"Vận tải","PVT":"Vận tải","HAH":"Vận tải","VSC":"Vận tải",
    "POW":"Năng lượng","NT2":"Năng lượng","GEG":"Năng lượng",
    "REE":"Năng lượng","PPC":"Năng lượng","VSH":"Năng lượng",
    "BVH":"Bảo hiểm","PVI":"Bảo hiểm","BMI":"Bảo hiểm",
    # Mã mới thêm 06/2026
    "EIB":"Ngân hàng","SSB":"Ngân hàng",
    "BCM":"Bất động sản","VPL":"Bất động sản",
    "GVR":"Cao su & Nông nghiệp",
    "SAB":"Tiêu dùng","DGC":"Hoá chất",
    "PNJ":"Bán lẻ trang sức",
    "GEX":"Điện & Vật liệu điện","GEL":"Điện & Vật liệu điện","GEE":"Điện & Vật liệu điện",
}
NAME_MAP = {
    "VCB":"Vietcombank","BID":"BIDV","CTG":"VietinBank","MBB":"MB Bank",
    "TCB":"Techcombank","ACB":"ACB","STB":"Sacombank","VPB":"VPBank",
    "HDB":"HDBank","VIB":"VIB","MSB":"MSB","LPB":"LPBank","SHB":"SHB",
    "OCB":"OCB","TPB":"TPBank","SSB":"SeABank",
    "VIC":"Vingroup","VHM":"Vinhomes","NVL":"Novaland","PDR":"Phát Đạt",
    "DXG":"Đất Xanh","KDH":"Khang Điền","BCM":"Becamex","VRE":"Vincom Retail",
    "SSI":"SSI","VND":"VNDIRECT","HCM":"HSC","MBS":"MBSecurities",
    "HPG":"Hòa Phát","HSG":"Hoa Sen","NKG":"Nam Kim",
    "FPT":"FPT","CMG":"CMC","GAS":"PV Gas","PLX":"Petrolimex",
    "PVD":"PV Drilling","PVS":"PV Technical",
    "VNM":"Vinamilk","MWG":"Thế Giới Di Động","MSN":"Masan","SAB":"Sabeco",
    "HVN":"Vietnam Airlines","VJC":"Vietjet","ACV":"Sân bay ACV",
    "GMD":"Gemadept","POW":"PV Power","REE":"REE Corp","BVH":"Bảo Việt",
    # Mã mới thêm 06/2026
    "EIB":"Eximbank","SSB":"SeABank","BCM":"Becamex IDC","VPL":"Vinpearl",
    "GVR":"Cao su Việt Nam","SAB":"Sabeco","DGC":"Hoá chất Đức Giang",
    "PNJ":"PNJ","GEX":"Gelex","GEL":"GEL","GEE":"GEE",
}
FALLBACK_FUND = {
    "VCB":{"pe":13.2,"pb":2.8,"roe":0.195,"roa":0.016,"eps":6820,"eps_growth":0.12},
    "BID":{"pe":10.1,"pb":1.5,"roe":0.142,"roa":0.007,"eps":4120,"eps_growth":0.18},
    "CTG":{"pe":9.8, "pb":1.4,"roe":0.148,"roa":0.009,"eps":3980,"eps_growth":0.15},
    "MBB":{"pe":7.5, "pb":1.3,"roe":0.218,"roa":0.018,"eps":5340,"eps_growth":0.22},
    "TCB":{"pe":8.2, "pb":1.4,"roe":0.196,"roa":0.022,"eps":6120,"eps_growth":0.19},
    "ACB":{"pe":8.9, "pb":1.7,"roe":0.245,"roa":0.020,"eps":5870,"eps_growth":0.17},
    "STB":{"pe":10.4,"pb":1.2,"roe":0.138,"roa":0.012,"eps":3210,"eps_growth":0.25},
    "VPB":{"pe":9.1, "pb":1.5,"roe":0.172,"roa":0.014,"eps":4580,"eps_growth":0.14},
    "HDB":{"pe":8.6, "pb":1.4,"roe":0.168,"roa":0.015,"eps":4210,"eps_growth":0.16},
    "VIB":{"pe":7.8, "pb":1.3,"roe":0.182,"roa":0.016,"eps":4820,"eps_growth":0.13},
    "VIC":{"pe":28.5,"pb":2.1,"roe":0.072,"roa":0.018,"eps":4250,"eps_growth":-0.05},
    "VHM":{"pe":11.2,"pb":1.8,"roe":0.162,"roa":0.048,"eps":7830,"eps_growth":0.34},
    "NVL":{"pe":35.2,"pb":1.2,"roe":0.032,"roa":0.008,"eps":1240,"eps_growth":-0.45},
    "PDR":{"pe":42.1,"pb":1.4,"roe":0.028,"roa":0.009,"eps":980, "eps_growth":-0.52},
    "VRE":{"pe":18.4,"pb":1.9,"roe":0.105,"roa":0.062,"eps":2840,"eps_growth":0.08},
    "HPG":{"pe":12.8,"pb":1.5,"roe":0.118,"roa":0.055,"eps":2870,"eps_growth":0.42},
    "HSG":{"pe":11.2,"pb":1.1,"roe":0.098,"roa":0.042,"eps":2340,"eps_growth":0.38},
    "FPT":{"pe":22.1,"pb":5.2,"roe":0.238,"roa":0.098,"eps":7420,"eps_growth":0.21},
    "GAS":{"pe":14.3,"pb":2.8,"roe":0.196,"roa":0.112,"eps":8940,"eps_growth":0.04},
    "PLX":{"pe":16.8,"pb":1.9,"roe":0.112,"roa":0.048,"eps":3420,"eps_growth":0.06},
    "SSI":{"pe":14.6,"pb":1.8,"roe":0.123,"roa":0.042,"eps":2340,"eps_growth":0.08},
    "VND":{"pe":12.4,"pb":1.6,"roe":0.132,"roa":0.045,"eps":2180,"eps_growth":0.12},
    "HCM":{"pe":13.8,"pb":2.1,"roe":0.152,"roa":0.058,"eps":2560,"eps_growth":0.15},
    "MWG":{"pe":16.8,"pb":3.2,"roe":0.192,"roa":0.058,"eps":5640,"eps_growth":1.25},
    "MSN":{"pe":45.2,"pb":3.8,"roe":0.082,"roa":0.028,"eps":2140,"eps_growth":0.18},
    "VNM":{"pe":17.2,"pb":3.6,"roe":0.208,"roa":0.148,"eps":4280,"eps_growth":0.05},
    "SAB":{"pe":22.4,"pb":4.2,"roe":0.188,"roa":0.142,"eps":7840,"eps_growth":0.03},
    "POW":{"pe":12.6,"pb":1.1,"roe":0.088,"roa":0.042,"eps":1240,"eps_growth":0.06},
    # Mã mới thêm 06/2026
    "EIB":{"pe":14.2,"pb":1.3,"roe":0.092,"roa":0.008,"eps":2100,"eps_growth":0.15},
    "SSB":{"pe":9.8, "pb":1.2,"roe":0.124,"roa":0.011,"eps":2850,"eps_growth":0.18},
    "BCM":{"pe":22.5,"pb":2.1,"roe":0.094,"roa":0.038,"eps":3200,"eps_growth":0.08},
    "VPL":{"pe":28.0,"pb":3.2,"roe":0.115,"roa":0.042,"eps":4800,"eps_growth":0.22},
    "GVR":{"pe":18.5,"pb":1.4,"roe":0.076,"roa":0.038,"eps":1850,"eps_growth":0.06},
    "SAB":{"pe":24.0,"pb":5.2,"roe":0.218,"roa":0.142,"eps":11200,"eps_growth":0.08},
    "DGC":{"pe":12.8,"pb":2.4,"roe":0.188,"roa":0.112,"eps":8500,"eps_growth":0.12},
    "PNJ":{"pe":16.5,"pb":3.8,"roe":0.232,"roa":0.118,"eps":6200,"eps_growth":0.15},
    "GEX":{"pe":18.0,"pb":1.6,"roe":0.089,"roa":0.032,"eps":1650,"eps_growth":0.10},
    "GEL":{},
    "GEE":{},
    "REE":{"pe":9.8, "pb":1.4,"roe":0.142,"roa":0.082,"eps":5280,"eps_growth":0.08},
    "BVH":{"pe":18.4,"pb":1.8,"roe":0.098,"roa":0.012,"eps":3420,"eps_growth":0.07},
    "HVN":{"pe":35.6,"pb":3.2,"roe":0.088,"roa":0.018,"eps":1840,"eps_growth":2.40},
    "VJC":{"pe":14.8,"pb":2.4,"roe":0.162,"roa":0.058,"eps":5640,"eps_growth":0.28},
    "ACV":{"pe":24.2,"pb":4.8,"roe":0.198,"roa":0.112,"eps":7840,"eps_growth":0.22},
    "GMD":{"pe":12.4,"pb":2.2,"roe":0.178,"roa":0.098,"eps":4280,"eps_growth":0.18},
    "NT2":{"pe":8.4, "pb":1.2,"roe":0.142,"roa":0.088,"eps":3840,"eps_growth":0.04},
    "BCM":{"pe":18.6,"pb":2.4,"roe":0.128,"roa":0.068,"eps":3240,"eps_growth":0.12},
    "KDH":{"pe":16.4,"pb":1.6,"roe":0.098,"roa":0.052,"eps":2840,"eps_growth":0.08},
    "DXG":{"pe":22.8,"pb":1.2,"roe":0.052,"roa":0.024,"eps":1240,"eps_growth":-0.28},
    "PVD":{"pe":14.2,"pb":0.9,"roe":0.062,"roa":0.032,"eps":2140,"eps_growth":0.45},
    "PVS":{"pe":11.4,"pb":1.1,"roe":0.098,"roa":0.048,"eps":2640,"eps_growth":0.18},
}

# ── Tính chỉ báo kỹ thuật ────────────────────────────────────────────────
def wilder(s, p):
    out = np.full(len(s), np.nan)
    buf, prev = [], None
    for i, v in enumerate(s):
        if np.isnan(v): continue
        if prev is None:
            buf.append(v)
            if len(buf) == p: prev = np.mean(buf); out[i] = prev
        else:
            prev = (prev*(p-1)+v)/p; out[i] = prev
    return out

def compute_signals(df):
    c = df["close"].values
    h = df["high"].values
    l = df["low"].values
    v = df["volume"].values
    n = len(c)

    # SMA
    def sma(s, p):
        return pd.Series(s).rolling(p, min_periods=p).mean().values
    def ema(s, p):
        return pd.Series(s).ewm(span=p, adjust=False).mean().values

    sma20 = sma(c, 20); sma50 = sma(c, 50)

    # RSI
    delta = np.diff(c, prepend=c[0])
    gain  = np.where(delta>0, delta, 0.0)
    loss  = np.where(delta<0, -delta, 0.0)
    ag = wilder(gain, 14); al = wilder(loss, 14)
    rsi = 100 - 100/(1 + np.where(al==0, np.inf, ag/al))

    # MACD
    ef = ema(c, 12); es = ema(c, 26)
    macd = ef - es
    sig  = pd.Series(macd).ewm(span=9, adjust=False).mean().values
    hist = macd - sig

    # Bollinger
    mid = sma(c, 20)
    sd  = pd.Series(c).rolling(20, min_periods=20).std().values
    bblo= mid - 2*sd; bbup = mid + 2*sd
    pctb= np.where((bbup-bblo)>0, (c-bblo)/(bbup-bblo), np.nan)

    # ADX
    up = np.diff(h, prepend=h[0]); dn = -np.diff(l, prepend=l[0])
    pdm = np.where((up>dn)&(up>0), up, 0.0)
    mdm = np.where((dn>up)&(dn>0), dn, 0.0)
    tr  = np.maximum(h-l, np.maximum(np.abs(h-np.roll(c,1)), np.abs(l-np.roll(c,1))))
    tr[0] = h[0]-l[0]
    atr_ = wilder(tr, 14)
    safe = np.where(atr_>0, atr_, np.nan)
    pdi  = 100*wilder(pdm,14)/safe
    mdi  = 100*wilder(mdm,14)/safe
    dx   = np.where((pdi+mdi)>0, 100*np.abs(pdi-mdi)/(pdi+mdi), np.nan)
    adx_ = wilder(dx, 14)

    # Volume
    volma = sma(v.astype(float), 20)
    obv   = np.cumsum(np.sign(np.diff(c, prepend=c[0]))*v)

    clip = lambda x: max(-1.0, min(1.0, x))

    # Score tại phiên cuối
    i = n - 1
    if i < 50 or np.isnan(sma20[i]) or np.isnan(sma50[i]):
        return None

    # Xu hướng (w=0.276)
    trend = 0.0
    trend += 0.5 if c[i]>sma50[i] else -0.5
    trend += 0.5 if sma20[i]>sma50[i] else -0.5
    if i>0:
        if sma20[i]>sma50[i] and sma20[i-1]<=sma50[i-1]: trend += 1
        if sma20[i]<sma50[i] and sma20[i-1]>=sma50[i-1]: trend -= 1
        if macd[i]>sig[i] and macd[i-1]<=sig[i-1]: trend += 1
        if macd[i]<sig[i] and macd[i-1]>=sig[i-1]: trend -= 1
    trend += 0.25 if hist[i]>0 else -0.25
    s_trend = clip(trend/3.25)

    # Động lượng (w=0.281)
    mom = 0.0
    if not np.isnan(rsi[i]):
        if rsi[i]<30: mom += 1.0
        elif rsi[i]>70: mom -= 1.0
        elif rsi[i]>=50: mom += 0.3
        else: mom -= 0.3
    s_mom = clip(mom/2.0)

    # Biến động (w=0.095)
    vol_s = 0.0
    if not np.isnan(pctb[i]):
        if pctb[i]<0.05: vol_s += 1.0
        elif pctb[i]>0.95: vol_s -= 1.0
        elif pctb[i]<0.5: vol_s += 0.2
        else: vol_s -= 0.2
    s_vol = clip(vol_s)

    # Khối lượng (w=0.173)
    vu = 0.0
    if not np.isnan(volma[i]) and volma[i]>0:
        spike = v[i] > 1.5*volma[i]
        up_day = c[i]>c[i-1] if i>0 else False
        if spike and up_day: vu += 0.6
        elif spike: vu -= 0.6
    if i>=5: vu += 0.4 if obv[i]>obv[i-5] else -0.4
    s_vol2 = clip(vu)

    # Sức mạnh (w=0.175)
    str_s = 0.0
    if not np.isnan(adx_[i]) and adx_[i]>25:
        if not np.isnan(pdi[i]) and not np.isnan(mdi[i]):
            if pdi[i]>mdi[i]: str_s += 0.8
            else: str_s -= 0.8
    s_str = clip(str_s)

    # Điểm tổng hợp (trọng số từ dữ liệu thật HOSE)
    total = (0.276*s_trend + 0.281*s_mom + 0.095*s_vol
           + 0.173*s_vol2  + 0.175*s_str)
    signal = "BUY" if total>=0.25 else "SELL" if total<=-0.25 else "HOLD"

    # GTGD TB 20 phiên (tỷ đồng)
    prices  = c[-20:] if len(c)>=20 else c
    volumes = v[-20:] if len(v)>=20 else v
    gtgd = float(np.mean(prices*volumes)/1e9)

    return {
        "signal":   signal,
        "score":    round(float(total), 3),
        "rsi":      round(float(rsi[i]),1) if not np.isnan(rsi[i]) else None,
        "adx":      round(float(adx_[i]),1) if not np.isnan(adx_[i]) else None,
        "macd_hist":round(float(hist[i]),0),
        "close":    int(c[i]),
        "chg_pct":  round((c[i]/c[i-1]-1)*100,2) if i>0 else 0,
        "gtgd_bn":  round(gtgd,1),
        "s_trend":  round(s_trend,3),
        "s_mom":    round(s_mom,3),
        "s_vol":    round(s_vol,3),
        "s_volume": round(s_vol2,3),
        "s_str":    round(s_str,3),
    }

# ── Hàm tải dữ liệu ─────────────────────────────────────────────────────
def normalize(df):
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    tcol = next((c for c in ("time","date","tradingdate") if c in df.columns), None)
    if tcol:
        df[tcol] = df[tcol].astype(str).str.slice(0,10)
        df = df.set_index(tcol)
    keep = [c for c in ("open","high","low","close","volume") if c in df.columns]
    return df[keep].dropna()

def fetch_ohlcv(symbol, start, end, source, retries=4):
    from vnstock.api.quote import Quote
    for attempt in range(retries):
        try:
            q = Quote(symbol=symbol, source=source)
            raw = q.history(start=start, end=end, interval="1D")
            df  = normalize(raw)
            if not df.empty: return df
            raise ValueError("empty")
        except Exception as e:
            msg = str(e).lower()
            if "rate limit" in msg or "429" in msg or "giới hạn" in msg:
                wait = DELAY*(attempt+1)
                print(f"    ⏳ rate limit — chờ {wait}s...", flush=True)
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Không tải được {symbol}")

def fetch_vn100_list():
    try:
        from vnstock import Listing
        df = Listing(source=SOURCE).symbols_by_group(group="VN100")
        if df is None or df.empty: raise ValueError("empty")
        df.columns = [c.lower() for c in df.columns]
        col = next((c for c in ("symbol","ticker") if c in df.columns), None)
        if col:
            syms = df[col].astype(str).str.upper().tolist()
            print(f"  Tải được danh sách VN100: {len(syms)} mã từ API")
            return syms
    except Exception as e:
        print(f"  VN100 API lỗi ({e}), dùng danh sách cứng ({len(VN100_FALLBACK)} mã)")
    return VN100_FALLBACK

def series(df):
    closes = df["close"].astype(float)
    scale  = 1000.0 if closes.median()<1000 else 1.0
    return {
        "dates":  list(df.index.astype(str)),
        "open":   [round(v*scale) for v in df["open"].astype(float)],
        "high":   [round(v*scale) for v in df["high"].astype(float)],
        "low":    [round(v*scale) for v in df["low"].astype(float)],
        "close":  [round(v*scale) for v in closes],
        "volume": [int(v) for v in df["volume"].astype(float)],
    }

# ── Tải tin tức vĩ mô từ Google Sheet (do Gemini cập nhật) ───────────────
def _deaccent(s):
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")

def fetch_news():
    """Đọc Sheet tin tức (CSV) -> {MÃ: {sentiment, text}}.
    Nếu Sheet chưa publish hoặc lỗi mạng, dùng tin vĩ mô thực tế được cập nhật hàng ngày.
    """
    # ── Tin vĩ mô thực tế (fallback / seed) — cập nhật 29/06/2026 ──────────
    SEED_NEWS = {
        # Ngân hàng
        "VCB": ("TÍCH CỰC", "[TÍCH CỰC] Thông tư 25/2026/TT-NHNN nâng tỷ lệ vốn ngắn hạn cho vay trung-dài hạn từ 30% lên 40%, tạo thêm dư địa tín dụng cho VCB. Dự báo lợi nhuận trước thuế Q2/2026 đạt 12.827 tỷ đồng (+16% YoY). VCB tiếp tục dẫn đầu hệ thống với vốn hóa ~18 tỷ USD và nợ xấu dưới 1,5%."),
        "BID": ("TÍCH CỰC", "[TÍCH CỰC] BIDV dự báo lợi nhuận Q2/2026 đạt 9.921–10.034 tỷ đồng (+15–16% YoY), hưởng lợi từ Thông tư 25/2026 và đẩy mạnh thu hồi nợ xấu đã xử lý. Tín dụng hướng vào hạ tầng và khu công nghiệp được ưu tiên trong nửa cuối năm."),
        "CTG": ("TÍCH CỰC", "[TÍCH CỰC] VietinBank dự báo lợi nhuận trước thuế Q2/2026 đạt 13.800–14.957 tỷ đồng (+14–24% YoY) — mức tăng trưởng cao nhất nhóm quốc doanh. Thông tư 25/2026 hỗ trợ mở rộng cho vay hạ tầng trọng điểm và cải thiện NIM trong H2/2026."),
        "MBB": ("TÍCH CỰC", "[TÍCH CỰC] MB Bank dự báo lợi nhuận Q2/2026 đạt 8.812 tỷ đồng (+18% YoY), nhờ tín dụng tăng ~35% và chi phí dự phòng được kiểm soát. Thông tư 25/2026 giúp MBB — ngân hàng có tỷ lệ SMLR tiệm cận 30% — mở rộng đáng kể dư địa cho vay trung-dài hạn."),
        "TCB": ("TÍCH CỰC", "[TÍCH CỰC] Techcombank hưởng lợi kép từ Thông tư 25/2026 và tệp khách hàng bất động sản phục hồi ở miền Bắc. Tăng trưởng tín dụng mục tiêu 18–20% được hỗ trợ bởi danh mục bất động sản và doanh nghiệp lớn. Chất lượng tài sản cải thiện với nợ xấu mục tiêu 1,3–1,4%."),
        "ACB": ("TÍCH CỰC", "[TÍCH CỰC] ACB được Yuanta Việt Nam đánh giá là cổ phiếu chất lượng với định giá hấp dẫn (P/B ~1,2x, thấp hơn trung bình lịch sử). ROE duy trì ~24,5% — cao nhất ngành. Thông tư 25/2026 tạo thêm không gian mở rộng tín dụng doanh nghiệp vừa và lớn."),
        "VPB": ("TÍCH CỰC", "[TÍCH CỰC] VPBank dự báo lợi nhuận Q2/2026 đạt 9.374 tỷ đồng (+51% YoY) — mức tăng trưởng mạnh nhất nhóm tư nhân lớn. Tín dụng tăng ~25%, thu nhập lãi thuần Q2 kỳ vọng tăng 34%. Hưởng lợi từ Thông tư 25/2026 và quan hệ đối tác chiến lược với SMBC."),
        "STB": ("TÍCH CỰC", "[TÍCH CỰC] Sacombank tiếp tục lộ trình xử lý nợ tồn đọng với kết quả tích cực trong H1/2026. Thông tư 25/2026 hỗ trợ mở rộng tín dụng trung-dài hạn, cải thiện NIM trong bối cảnh áp lực chi phí vốn dần ổn định."),
        "HDB": ("TÍCH CỰC", "[TÍCH CỰC] HDBank dự báo lợi nhuận Q2/2026 vượt 7.000 tỷ đồng (+50% YoY), nhờ tín dụng tăng ~20% và chi phí dự phòng giảm mạnh. Thông tư 25/2026 tạo lợi thế bổ sung cho ngân hàng có tỷ lệ SMLR cao."),
        "LPB": ("TRUNG LẬP", "[TRUNG LẬP] LPBank đang trong giai đoạn chuyển đổi mô hình kinh doanh và tăng vốn điều lệ. Hưởng lợi từ Thông tư 25/2026 nhưng quy mô nhỏ hơn hạn chế tốc độ mở rộng. Kết quả kinh doanh Q2/2026 chưa có dự báo cụ thể từ các tổ chức phân tích lớn."),
        "VIB": ("TÍCH CỰC", "[TÍCH CỰC] VIB được NHNN chấp thuận tăng vốn điều lệ thêm 3.313 tỷ đồng, củng cố năng lực tài chính và tỷ lệ CAR. Thông tư 25/2026 hỗ trợ ngân hàng có tỷ lệ SMLR tiệm cận ngưỡng 30% mở rộng dư địa cho vay."),
        "MSB": ("TÍCH CỰC", "[TÍCH CỰC] MSB ghi nhận tín hiệu kỹ thuật tích cực với ADX >45 cho thấy xu hướng tăng mạnh. Thông tư 25/2026 tạo dư địa mở rộng tín dụng trung-dài hạn. Cổ phiếu tăng 0,95% trong tuần với thanh khoản cải thiện."),
        "SHB": ("TRUNG LẬP", "[TRUNG LẬP] SHB công bố kế hoạch chào bán 3.000 tỷ đồng trái phiếu để bổ sung vốn cấp 2, hỗ trợ tỷ lệ an toàn vốn. Hưởng lợi từ Thông tư 25/2026 nhưng áp lực chi phí vốn từ phát hành trái phiếu có thể ảnh hưởng NIM ngắn hạn."),
        "OCB": ("TÍCH CỰC", "[TÍCH CỰC] OCB ghi nhận cải thiện chất lượng tài sản với tín hiệu kỹ thuật MUA (ADX >30). Thông tư 25/2026 hỗ trợ mở rộng cho vay trung-dài hạn trong bối cảnh định giá cổ phiếu còn hấp dẫn."),
        "TPB": ("TRUNG LẬP", "[TRUNG LẬP] TPBank trong vùng tích lũy sau giai đoạn tăng vốn. Hưởng lợi từ Thông tư 25/2026 nhưng chưa có xúc tác rõ ràng để bứt phá. RSI quanh 51 cho thấy đà tăng chưa đủ mạnh để duy trì xu hướng tăng."),
        # Bất động sản
        "VIC": ("TÍCH CỰC", "[TÍCH CỰC] Vingroup hưởng lợi trực tiếp từ danh mục dự án trọng điểm được hỗ trợ vốn theo Thông tư 25/2026, với tổng đầu tư hơn 750.000 tỷ đồng từ các tập đoàn lớn. VIC tăng 1,33% trong tuần với khối ngoại mua ròng mạnh hơn 155 tỷ đồng."),
        "VHM": ("TÍCH CỰC", "[TÍCH CỰC] Vinhomes tăng 3,51% trong tuần — mức tăng mạnh nhất nhóm BĐS lớn — nhờ dòng tiền tổ chức trong nước mua vào. Thông tư 25/2026 hỗ trợ gián tiếp qua việc các ngân hàng đối tác mở rộng cho vay dự án Vinhomes."),
        "NVL": ("TIÊU CỰC", "[TIÊU CỰC] Novaland tiếp tục đối mặt với áp lực tái cơ cấu nợ và pháp lý dự án. RSI giảm xuống 40, ADX >32 cho thấy xu hướng giảm đang mạnh lên. P/E ở mức 35x trong bối cảnh lợi nhuận sụt giảm tạo rủi ro định giá cao."),
        "PDR": ("TIÊU CỰC", "[TIÊU CỰC] Phát Đạt chịu áp lực từ tiến độ pháp lý dự án chậm và dòng tiền bị siết. RSI giảm về 36, P/E ở mức 42x so với lợi nhuận phục hồi chậm. Giá giảm 0,34% trong bối cảnh thanh khoản thấp."),
        "DXG": ("TIÊU CỰC", "[TIÊU CỰC] Đất Xanh trong xu hướng điều chỉnh ngắn hạn với RSI 38 và điểm tín hiệu âm. Doanh nghiệp đang tập trung xử lý hàng tồn kho và tối ưu dòng tiền trong bối cảnh thị trường BĐS phục hồi chậm ở phân khúc tầm trung."),
        "KDH": ("TIÊU CỰC", "[TIÊU CỰC] Khang Điền chịu áp lực kỹ thuật với RSI chạm mức 30 (vùng quá bán) và MACD hist âm sâu. Tuy nhiên định giá P/B 1,6x và ROE ~10% cùng quỹ đất sạch tạo hỗ trợ nền giá. Có thể phục hồi kỹ thuật nếu thị trường BĐS ấm lên."),
        "VRE": ("TRUNG LẬP", "[TRUNG LẬP] Vincom Retail duy trì tăng trưởng doanh thu ổn định từ mảng cho thuê bán lẻ. Cổ phiếu tăng 1,35% trong tuần nhưng điểm tổng hợp vẫn âm nhẹ do momentum ngắn hạn chưa đủ mạnh. P/E 18,4x ở mức hợp lý so với tăng trưởng thu nhập."),
        "NLG": ("TRUNG LẬP", "[TRUNG LẬP] Nam Long tăng 3,93% trong tuần — một trong những mức tăng mạnh nhất nhóm BĐS — hưởng lợi từ dòng tiền tổ chức. Tuy nhiên khối lượng giao dịch còn thấp và xu hướng kỹ thuật dài hạn chưa đảo chiều."),
        "DIG": ("TIÊU CỰC", "[TIÊU CỰC] DIG nhận tín hiệu BÁN với RSI 34 và điểm âm -0,283. Tiến độ giải phóng mặt bằng và pháp lý dự án tại Bà Rịa – Vũng Tàu vẫn chậm, gây áp lực dòng tiền ngắn hạn."),
        "CII": ("TIÊU CỰC", "[TIÊU CỰC] CII giảm 0,29% với tín hiệu kỹ thuật yếu (RSI 44, điểm âm). Doanh nghiệp hạ tầng giao thông đang chờ đẩy nhanh tiến độ dự án sau khi nguồn vốn trung-dài hạn được nới lỏng theo Thông tư 25/2026."),
        "VPI": ("TRUNG LẬP", "[TRUNG LẬP] VPI trong giai đoạn tích lũy với tín hiệu trung lập. Dự án tại Hà Nội đang đẩy nhanh pháp lý. Cổ phiếu giảm nhẹ 0,64% nhưng RSI 55 cho thấy áp lực bán chưa mạnh."),
        # Công nghệ
        "FPT": ("TÍCH CỰC", "[TÍCH CỰC] FPT — công ty tư nhân lớn nhất sàn chứng khoán với vốn hóa ~205.000 tỷ đồng — tiếp tục tăng trưởng mạnh từ mảng xuất khẩu phần mềm và AI. SSI Research dự báo lợi nhuận tiếp tục tăng trưởng nhờ đơn hàng từ Nhật Bản, Mỹ và thị trường AI toàn cầu."),
        # Dầu khí
        "GAS": ("TIÊU CỰC", "[TIÊU CỰC] PV Gas giảm 0,65% trong tuần với RSI 38 và tín hiệu kỹ thuật trung lập âm. Giá khí LNG toàn cầu biến động trong bối cảnh bất định địa chính trị. Cổ tức cao (P/E 14,3x) là điểm hỗ trợ nhưng giá dầu giảm tạo rủi ro doanh thu."),
        "PLX": ("TIÊU CỰC", "[TIÊU CỰC] Petrolimex giảm 1,21% trong tuần với RSI xuống 36. Biên lợi nhuận chịu áp lực từ thuế môi trường và điều chỉnh giá xăng dầu trong nước. SSI Research dự báo lợi nhuận giảm trong 2026."),
        "PVD": ("TÍCH CỰC", "[TÍCH CỰC] PV Drilling hưởng lợi từ hoạt động khoan thăm dò tăng mạnh tại thị trường nước ngoài. RSI 53 và ADX 19 cho thấy đà phục hồi đang hình thành. EPS tăng 45% YoY là điểm sáng kỹ thuật cơ bản."),
        "PVS": ("TIÊU CỰC", "[TIÊU CỰC] PV Technical nhận tín hiệu BÁN với RSI 44 và điểm âm -0,283. Biên lợi nhuận dịch vụ dầu khí bị thu hẹp trong bối cảnh chi phí vận hành tăng. Giá dầu Brent biến động là rủi ro chính."),
        "BSR": ("TIÊU CỰC", "[TIÊU CỰC] BSR giảm 1,42% trong tuần với RSI chạm vùng quá bán (33). Biên lọc dầu chịu áp lực từ chênh lệch crack spread thu hẹp. Doanh nghiệp đang chờ kế hoạch nâng cấp nhà máy lọc dầu Dung Quất giai đoạn 2."),
        # Chứng khoán
        "SSI": ("TIÊU CỰC", "[TIÊU CỰC] SSI nhận tín hiệu BÁN với RSI 40 và điểm âm -0,283. Thị trường chứng khoán tuần qua chịu áp lực từ khối ngoại bán ròng liên tiếp. Khối tự doanh và ngoại bán ròng gần 693 tỷ đồng trong một phiên, ảnh hưởng trực tiếp đến doanh thu môi giới."),
        "VND": ("TRUNG LẬP", "[TRUNG LẬP] VNDIRECT ở vùng tích lũy với RSI 53 và điểm tín hiệu gần trung tính. Thị trường chứng khoán biến động tạo doanh thu môi giới không ổn định. Chiến lược đẩy mạnh dịch vụ tư vấn doanh nghiệp là hướng tăng trưởng dài hạn."),
        "HCM": ("TIÊU CỰC", "[TIÊU CỰC] HSC giảm 0,19% trong tuần với điểm tín hiệu âm -0,199. Áp lực bán từ khối ngoại và tự doanh ảnh hưởng đến doanh thu môi giới. Cổ phiếu đang trong giai đoạn điều chỉnh ngắn hạn sau đợt tăng trước đó."),
        "MBS": ("TIÊU CỰC", "[TIÊU CỰC] MBSecurities giảm 0,51% với tín hiệu kỹ thuật yếu (RSI 45, điểm âm). Môi trường thị trường ít thuận lợi cho các công ty chứng khoán vừa trong bối cảnh khối ngoại bán ròng liên tục."),
        # Thép
        "HPG": ("TIÊU CỰC", "[TIÊU CỰC] Hòa Phát chịu áp lực từ giá thép Trung Quốc giảm mạnh và cạnh tranh nhập khẩu. RSI 45 và điểm âm -0,199. Tuy nhiên EPS tăng 42% YoY và P/E 12,8x cho thấy định giá hấp dẫn nếu giá thép phục hồi trong H2/2026."),
        # Tiêu dùng
        "VNM": ("TIÊU CỰC", "[TIÊU CỰC] Vinamilk nhận tín hiệu BÁN với RSI 39 và điểm âm -0,283. Sức tiêu thụ nội địa phục hồi chậm hơn kỳ vọng và áp lực từ chi phí nguyên liệu đầu vào. SSI Research dự báo lợi nhuận tăng trưởng khiêm tốn trong 2026."),
        "MSN": ("TIÊU CỰC", "[TIÊU CỰC] Masan nhận tín hiệu BÁN mạnh với ADX >37 xác nhận xu hướng giảm. P/E 45x rất cao so với ROE 8,2%. Áp lực từ mảng khai khoáng (Masan High-Tech Materials) và kỳ vọng tái cơ cấu chưa rõ thời điểm."),
        # Bán lẻ
        "MWG": ("TÍCH CỰC", "[TÍCH CỰC] Thế Giới Di Động tăng 1,68% trong tuần với EPS tăng trưởng 125% YoY — mức phục hồi ấn tượng nhất ngành bán lẻ. Chuỗi Bách Hóa Xanh đang tạo đột biến doanh thu trong bối cảnh sức mua người dùng hồi phục."),
        # Hàng không
        "VJC": ("TÍCH CỰC", "[TÍCH CỰC] Vietjet ghi nhận tín hiệu tích cực với RSI 54 và điểm tổng hợp dương +0,158. Mùa du lịch hè 2026 bùng nổ với hệ số tải chở đạt mức cao kỷ lục. EPS tăng 28% YoY nhờ phục hồi du lịch nội địa và quốc tế mạnh mẽ."),
        # Vận tải
        "GMD": ("TIÊU CỰC", "[TIÊU CỰC] Gemadept giảm nhẹ với RSI 39 và MACD hist âm sâu -495. Lưu lượng container qua cảng tăng nhưng áp lực cạnh tranh phí cảng và chi phí vận hành tăng thu hẹp biên lợi nhuận trong Q2/2026."),
        "VSC": ("TIÊU CỰC", "[TIÊU CỰC] VSC nhận tín hiệu BÁN với ADX >33 xác nhận xu hướng giảm. Thị trường vận tải container nội địa cạnh tranh gay gắt. Doanh nghiệp đang tái cơ cấu đội tàu và tối ưu chi phí khai thác."),
        # Năng lượng
        "POW": ("TÍCH CỰC", "[TÍCH CỰC] PV Power tăng 2,07% trong tuần với điểm tổng hợp dương cao nhất nhóm năng lượng (+0,226). Nhu cầu điện mùa hè tăng mạnh và giá điện được điều chỉnh tăng hỗ trợ doanh thu. RSI 65 cho thấy đà tăng đang duy trì."),
        # Mã mới thêm 06/2026
        "EIB": ("TRUNG LẬP", "[TRUNG LẬP] Eximbank đang trong quá trình củng cố quản trị sau giai đoạn biến động lãnh đạo. Hưởng lợi từ Thông tư 25/2026 nhưng quy mô tín dụng còn hạn chế so với nhóm ngân hàng lớn. Kết quả kinh doanh Q2/2026 cần theo dõi thêm để xác nhận đà phục hồi."),
        "SSB": ("TÍCH CỰC", "[TÍCH CỰC] SeABank (SSB) là thành phần VN30 với tăng trưởng tín dụng duy trì ổn định. Hưởng lợi từ Thông tư 25/2026 và nhu cầu vốn trung-dài hạn tăng. Định giá P/B ~1,2x ở mức hấp dẫn so với ROE ~12,4%."),
        "BCM": ("TRUNG LẬP", "[TRUNG LẬP] Becamex IDC (BCM) hưởng lợi từ làn sóng FDI vào khu công nghiệp Bình Dương. Tuy nhiên tỷ lệ free-float thấp hạn chế thanh khoản và là lý do BCM bị loại khỏi VN30 trong kỳ tháng 1/2026. Dài hạn vẫn là mã có nền tảng tốt từ quỹ đất khu công nghiệp."),
        "VPL": ("TÍCH CỰC", "[TÍCH CỰC] Vinpearl (VPL) vừa gia nhập VN30 từ tháng 2/2026 với vốn hóa lọt Top 20 thị trường. Du lịch hè 2026 bùng nổ hỗ trợ trực tiếp doanh thu mảng nghỉ dưỡng và vui chơi giải trí. Cổ phiếu còn non trẻ trên sàn nhưng được kỳ vọng tăng trưởng mạnh nhờ hệ sinh thái Vingroup."),
        "GVR": ("TRUNG LẬP", "[TRUNG LẬP] Cao su Việt Nam (GVR) được hưởng lợi từ giá cao su tự nhiên phục hồi nhẹ trong Q2/2026. Tuy nhiên biên lợi nhuận mỏng và phụ thuộc lớn vào giá cao su thế giới tạo rủi ro doanh thu. Quỹ đất cao su chuyển đổi sang khu công nghiệp là điểm sáng dài hạn."),
        "SAB": ("TRUNG LẬP", "[TRUNG LẬP] Sabeco (SAB) ghi nhận doanh thu phục hồi trong mùa hè nhờ tiêu dùng nội địa cải thiện. Tuy nhiên cạnh tranh từ bia nhập khẩu và thói quen tiêu dùng thay đổi tạo áp lực. P/E 24x ở mức cao trong ngành đồ uống."),
        "DGC": ("TÍCH CỰC", "[TÍCH CỰC] Hoá chất Đức Giang (DGC) hưởng lợi từ nhu cầu phốt pho vàng và các sản phẩm hoá chất cao cấp tăng mạnh từ thị trường xuất khẩu. EPS tăng trưởng mạnh nhờ giá bán cải thiện. MBS dự báo là ứng cử viên tái gia nhập VN30 trong kỳ tháng 7/2026."),
        "PNJ": ("TÍCH CỰC", "[TÍCH CỰC] PNJ hưởng lợi từ sức mua trang sức phục hồi trong dịp lễ và mùa cưới Q2/2026. ROE ~23% thuộc nhóm cao nhất ngành bán lẻ. Chuỗi cửa hàng mở rộng liên tục và mảng kinh doanh vàng hưởng lợi từ giá vàng tăng cao."),
        "GEX": ("TÍCH CỰC", "[TÍCH CỰC] Tập đoàn Gelex (GEX) công bố doanh thu Q1/2026 hơn 10.700 tỷ đồng (+35% YoY), lợi nhuận trước thuế khoảng 806 tỷ đồng. Vừa rót hơn 8.000 tỷ đồng vào hạ tầng sân bay Gia Bình qua liên doanh, mở rộng mảng đầu tư hạ tầng. Cổ phiếu đang ở vùng giá cao nhất từ đầu năm sau đợt phát hành cổ phiếu thưởng tỷ lệ 45%."),
        "GEL": ("TÍCH CỰC", "[TÍCH CỰC] GELEX Hạ tầng (GEL) vừa niêm yết từ 06/02/2026, sở hữu chi phối Viglacera với mảng vật liệu xây dựng và bất động sản khu công nghiệp. Vừa ký hợp tác chiến lược với Frasers Property mở rộng đầu tư bất động sản dân dụng, đồng thời tham gia 20% cổ phần dự án sân bay quốc tế Gia Bình — tạo động lực tăng trưởng dài hạn."),
        "GEE": ("TÍCH CỰC", "[TÍCH CỰC] GELEX Electric (GEE) vừa hoàn tất phát hành cổ phiếu thưởng tỷ lệ 4:3, tăng vốn điều lệ lên ~6.404 tỷ đồng. Cổ phiếu duy trì xu hướng tăng trung hạn với thanh khoản tốt (~1 triệu cp/phiên). Sở hữu các thương hiệu thiết bị điện đầu ngành như Cadivi, Thibidi, cùng danh mục năng lượng tái tạo (thủy điện, điện mặt trời, điện gió)."),
    }

    news = {}
    if not NEWS_CSV_URL or "PASTE" in NEWS_CSV_URL:
        print("  (Chưa cấu hình NEWS_CSV_URL — dùng tin vĩ mô mặc định)")
        for tk, (sent, txt) in SEED_NEWS.items():
            news[tk] = {"sentiment": sent, "text": txt}
        return news
    try:
        import urllib.request, csv, io
        req = urllib.request.Request(NEWS_CSV_URL,
                                     headers={"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
        rows = list(csv.reader(io.StringIO(raw)))
        for r in rows[1:]:
            if len(r) < 2:
                continue
            tk  = r[0].strip().upper()
            txt = r[1].strip()
            if not tk or not txt:
                continue
            low = txt.lower()
            if low.startswith("loi") or low.startswith("lỗi") or "dang phan tich" in low or "đang phân tích" in low:
                continue
            head = _deaccent(txt[:50]).upper()
            if   "TICH CUC" in head: sentiment = "TÍCH CỰC"
            elif "TIEU CUC" in head: sentiment = "TIÊU CỰC"
            else:                    sentiment = "TRUNG LẬP"
            news[tk] = {"sentiment": sentiment, "text": txt}
        # Bổ sung seed cho những mã chưa có trong Sheet
        for tk, (sent, txt) in SEED_NEWS.items():
            if tk not in news:
                news[tk] = {"sentiment": sent, "text": txt}
        print(f"  Tải tin tức vĩ mô: {len(news)} mã có tin")
    except Exception as e:
        print(f"  Lỗi tải tin tức ({e}) — dùng tin vĩ mô mặc định")
        for tk, (sent, txt) in SEED_NEWS.items():
            news[tk] = {"sentiment": sent, "text": txt}
    return news

# ── MAIN ─────────────────────────────────────────────────────────────────
def main():
    end = dt.date.today().isoformat()
    print(f"Tải dữ liệu VN100 {START}→{end} | lọc ≥{LIQ_MIN_BILLION}tỷ/phiên\n")

    syms = fetch_vn100_list()
    print(f"  Danh sách: {len(syms)} mã\n")

    news = fetch_news()        # tin tức vĩ mô từ Google Sheet (Gemini)

    signals, stocks, fundamentals, universe = {}, {}, {}, {}
    ok_price = 0
    req_count = 0

    for i, sym in enumerate(syms):
        if req_count>0 and req_count%REQ_PER_BATCH==0:
            print(f"\n  ⏳ {req_count} requests — chờ {DELAY}s...\n", flush=True)
            time.sleep(DELAY)

        print(f"  [{sym:5s}]", end=" ", flush=True)
        try:
            df = fetch_ohlcv(sym, START, end, SOURCE)
            req_count += 1
            if len(df) < 60:
                print("ít dữ liệu, bỏ")
                continue

            # Lọc thanh khoản — vnstock 4.x trả giá đơn vị nghìn đồng
            sc = df["close"].values[-20:] if len(df)>=20 else df["close"].values
            sv = df["volume"].values[-20:] if len(df)>=20 else df["volume"].values
            price_scale = 1000.0 if float(np.median(sc)) < 1000 else 1.0
            gtgd = float(np.mean(sc.astype(float)*price_scale*sv.astype(float))/1e9)
            if gtgd < LIQ_MIN_BILLION:
                print(f"GTGD {gtgd:.0f}tỷ < {LIQ_MIN_BILLION}tỷ, bỏ")
                time.sleep(INTER_DELAY); continue

            # Scale giá về đơn vị đồng trước khi tính tín hiệu
            df_scaled = df.copy()
            if price_scale > 1:
                for col in ("open","high","low","close"):
                    df_scaled[col] = df_scaled[col] * price_scale

            # Tính tín hiệu
            sig = compute_signals(df_scaled)
            if sig is None:
                print("chưa đủ chỉ báo, bỏ")
                time.sleep(INTER_DELAY); continue

            # Ghi đè gtgd đã tính đúng vào sig
            sig["gtgd_bn"] = round(gtgd, 1)

            # Cơ bản
            fund = FALLBACK_FUND.get(sym, {})

            signals[sym]      = sig
            stocks[sym]       = series(df_scaled)
            fundamentals[sym] = fund
            universe[sym]     = {
                "name":   NAME_MAP.get(sym, sym),
                "sector": SECTOR_MAP.get(sym, "Khác"),
            }
            ok_price += 1
            print(f"✔ {sig['signal']:4s} score={sig['score']:+.3f} "
                  f"GTGD={gtgd:.0f}tỷ")
        except Exception as ex:
            print(f"lỗi: {ex}")

        time.sleep(INTER_DELAY)

    if ok_price == 0:
        print("❌ Không tải được mã nào"); sys.exit(1)

    # Xuất signals.js (nhỏ, bảng tín hiệu)
    sig_payload = json.dumps(
        {"asof": end, "liq_min": LIQ_MIN_BILLION,
         "signals": signals, "universe": universe,
         "fundamentals": fundamentals, "news": news},
        ensure_ascii=False, separators=(",",":"))
    with open(OUT_SIGNALS,"w",encoding="utf-8") as f:
        f.write("window.HOSE_SIGNALS = " + sig_payload + ";\n")

    # Xuất data.js (OHLCV đầy đủ, cho tab chi tiết)
    data_payload = json.dumps(
        {"asof": end, "universe": universe,
         "stocks": stocks, "fundamentals": fundamentals,
         "signals": signals, "news": news},
        ensure_ascii=False, separators=(",",":"))
    with open(OUT_DATA,"w",encoding="utf-8") as f:
        f.write("window.HOSE_DATA = " + data_payload + ";\n")
    with open(OUT_JSON,"w",encoding="utf-8") as f:
        f.write(data_payload)

    n_buy  = sum(1 for s in signals.values() if s["signal"]=="BUY")
    n_sell = sum(1 for s in signals.values() if s["signal"]=="SELL")
    sz = round(len(data_payload)/1e6, 1)
    print(f"\n✅ {ok_price} mã | MUA:{n_buy} BÁN:{n_sell} GIỮ:{ok_price-n_buy-n_sell}")
    print(f"   signals.js: {round(len(sig_payload)/1e3)}KB | data.js: {sz}MB")

if __name__=="__main__":
    main()
