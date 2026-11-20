import os
import sys
import time
import html
import socket
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import urllib3.util.connection as urllib3_cn

urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

BINANCE_BASE = "https://data-api.binance.vision"
HL_URL = "https://api.hyperliquid.xyz/info"

COINS = [
    ("BTCUSDT", "BTC", 1),
    ("ETHUSDT", "ETH", 1),
    ("SOLUSDT", "SOL", 1),
    ("XRPUSDT", "XRP", 1),
    ("BNBUSDT", "BNB", 1),
    ("DOGEUSDT", "DOGE", 1),
    ("SUIUSDT", "SUI", 1),
    ("AVAXUSDT", "AVAX", 1),
    ("LINKUSDT", "LINK", 1),
    ("ADAUSDT", "ADA", 1),
    ("LTCUSDT", "LTC", 1),
    ("NEARUSDT", "NEAR", 1),
    ("ARBUSDT", "ARB", 1),
    ("APTUSDT", "APT", 1),
    ("PEPEUSDT", "kPEPE", 1000),
]

TIMEFRAME = "1h"
FRESH_WINDOW_MS = 14 * 60 * 1000
MIN_CANDLE_SCORE = 5   # قفل‌شده روی سبد برنده (+0.26R در بک‌تست)
MAX_WORKERS = 8

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "OrderFlow-M5-Live/1.0"})


def fmt_price(px: float) -> str:
    if px >= 1000:
        return f"{px:,.2f}"
    if px >= 1:
        return f"{px:,.4f}"
    if px >= 0.01:
        return f"{px:,.5f}"
    return f"{px:,.8f}"


def fmt_usd(val: float) -> str:
    if val >= 1_000_000_000:
        return f"${val / 1_000_000_000:.2f}B"
    if val >= 1_000_000:
        return f"${val / 1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val / 1_000:.1f}K"
    return f"${val:.0f}"


def fetch_hl_contexts() -> dict:
    for attempt in range(3):
        try:
            r = SESSION.post(
                HL_URL,
                json={"type": "metaAndAssetCtxs"},
                timeout=12,
            )
            if r.status_code == 200:
                data = r.json()
                uni = data[0].get("universe", [])
                ctxs = data[1]
                return {
                    m["name"]: c
                    for m, c in zip(uni, ctxs)
                    if m.get("name")
                }
        except Exception:
            pass
        time.sleep(1.0 * (attempt + 1))
    return {}


def fetch_klines(symbol: str, tf: str = "1h", limit: int = 160) -> list:
    url = f"{BINANCE_BASE}/api/v3/klines"
    for _ in range(2):
        try:
            r = SESSION.get(
                url,
                params={"symbol": symbol, "interval": tf, "limit": limit},
                timeout=10,
            )
            if r.status_code == 200:
                out = []
                for k in r.json():
                    qv = float(k[7])
                    bqv = float(k[10])
                    sqv = max(qv - bqv, 0.0)
                    out.append({
                        "open_time": int(k[0]),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "close_time": int(k[6]),
                        "qvol": qv,
                        "delta_usdt": bqv - sqv,
                    })
                return out
        except Exception:
            pass
        time.sleep(0.5)
    return []


def fetch_spot_ob(symbol: str):
    url = f"{BINANCE_BASE}/api/v3/depth"
    try:
        r = SESSION.get(
            url,
            params={"symbol": symbol, "limit": 500},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        book = r.json()
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None

        bid_floor = float(bids[0][0]) * 0.995
        ask_ceil = float(asks[0][0]) * 1.005
        bid_usd = sum(
            float(p) * float(q) for p, q in bids if float(p) >= bid_floor
        )
        ask_usd = sum(
            float(p) * float(q) for p, q in asks if float(p) <= ask_ceil
        )
        if bid_usd <= 0 or ask_usd <= 0:
            return None
        return bid_usd, ask_usd, (bid_usd / ask_usd)
    except Exception:
        return None


def calc_atr(candles: list, period: int = 14) -> float:
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[-period:]) / period
    return max(atr, candles[-1]["close"] * 0.0005)


def calc_vp(candles: list, bins: int = 32):
    sub = candles[-48:]
    p_min = min(c["low"] for c in sub)
    p_max = max(c["high"] for c in sub)
    if p_max <= p_min:
        return p_min, p_min, p_min

    step = (p_max - p_min) / bins
    bvols = [0.0] * bins
    for c in sub:
        lo_i = max(0, min(bins - 1, int((c["low"] - p_min) / step)))
        hi_i = max(0, min(bins - 1, int((c["high"] - p_min) / step)))
        share = c["qvol"] / (hi_i - lo_i + 1)
        for idx in range(lo_i, hi_i + 1):
            bvols[idx] += share

    total = sum(bvols)
    poc_i = max(range(bins), key=lambda i: bvols[i])
    poc = p_min + (poc_i + 0.5) * step
    acc = bvols[poc_i]
    left, right = poc_i, poc_i

    while acc < total * 0.70 and (left > 0 or right < bins - 1):
        vl = bvols[left - 1] if left > 0 else -1.0
        vr = bvols[right + 1] if right < bins - 1 else -1.0
        if vl >= vr and left > 0:
            left -= 1
            acc += bvols[left]
        elif right < bins - 1:
            right += 1
            acc += bvols[right]
        else:
            break

    val = p_min + left * step
    vah = p_min + (right + 1) * step
    return poc, vah, val


def calc_vwap(candles: list, now_ms: int) -> float:
    day0 = (now_ms // 86_400_000) * 86_400_000
    sub = [c for c in candles if c["open_time"] >= day0]
    if len(sub) < 4:
        sub = candles[-24:]
    num = sum(
        ((c["high"] + c["low"] + c["close"]) / 3.0) * c["qvol"]
        for c in sub
    )
    den = sum(c["qvol"] for c in sub)
    return (num / den) if den > 0 else candles[-1]["close"]


def evaluate_m5(direction, closed, atr, poc, vah, val, vwap):
    curr = closed[-1]
    crange = curr["high"] - curr["low"]
    if crange < 0.50 * atr or curr["qvol"] <= 0:
        return None

    cpos = (curr["close"] - curr["low"]) / crange
    uwick = (curr["high"] - max(curr["open"], curr["close"])) / crange
    lwick = (min(curr["open"], curr["close"]) - curr["low"]) / crange
    avg_qvol = sum(c["qvol"] for c in closed[-21:-1]) / 20.0
    vratio = curr["qvol"] / max(avg_qvol, 1.0)
    dpct = curr["delta_usdt"] / curr["qvol"]

    p48_lo = min(c["low"] for c in closed[-49:-1])
    p48_hi = max(c["high"] for c in closed[-49:-1])

    sub = closed[-50:]
    cvd, run = [], 0.0
    for c in sub:
        run += c["delta_usdt"]
        cvd.append(run)
    avg_ad = sum(abs(c["delta_usdt"]) for c in sub[:-1]) / 49.0

    reasons = []
    sw48 = ab_s = cv_s = lv_p = 0

    if direction == "LONG":
        if (
            curr["low"] < p48_lo - 0.10 * atr
            and curr["close"] > p48_lo
            and cpos >= 0.55
        ):
            sw48 = 2
            reasons.append("شکار لیکوئیدیتی ۴۸ ساعته کف (Sweep48)")

        if vratio >= 1.9 and lwick >= 0.42 and cpos >= 0.52 and dpct <= -0.06:
            ab_s = 2
            reasons.append(f"جذب لیمیت خرید + شدوی برگشتی ({vratio:.1f}x)")
        elif vratio >= 1.9 and lwick >= 0.35 and cpos >= 0.62 and dpct >= 0.08:
            ab_s = 2
            reasons.append(f"حمله خریدار + اسپایک حجم ({vratio:.1f}x)")
        elif vratio >= 1.5 and lwick >= 0.42 and cpos >= 0.55:
            ab_s = 1
            reasons.append(f"ریجکت صعودی با حجم بالا ({vratio:.1f}x)")

        r_i = min(range(47, 50), key=lambda i: sub[i]["low"])
        p_i = min(range(9, 45), key=lambda i: sub[i]["low"])
        if (
            r_i >= 48
            and cpos >= 0.50
            and sub[r_i]["low"] < sub[p_i]["low"]
            and cvd[-1] > cvd[p_i] + 0.80 * avg_ad
            and cvd[r_i] > cvd[p_i]
        ):
            cv_s = 2
            reasons.append("واگرایی صعودی قطعی Spot CVD")

        lvls = [
            n for n, v in [("VAL", val), ("POC", poc), ("VWAP", vwap)]
            if curr["low"] <= v + 0.30 * atr and curr["close"] >= v - 0.15 * atr
        ]
        if lvls:
            lv_p = 1
            reasons.append(f"حمایت روی سطح {'/'.join(lvls)}")
        sl_px = curr["low"] - 0.45 * atr
    else:
        if (
            curr["high"] > p48_hi + 0.10 * atr
            and curr["close"] < p48_hi
            and cpos <= 0.45
        ):
            sw48 = 2
            reasons.append("شکار لیکوئیدیتی ۴۸ ساعته سقف (Sweep48)")

        if vratio >= 1.9 and uwick >= 0.42 and cpos <= 0.48 and dpct >= 0.06:
            ab_s = 2
            reasons.append(f"جذب لیمیت فروش + شدوی برگشتی ({vratio:.1f}x)")
        elif vratio >= 1.9 and uwick >= 0.35 and cpos <= 0.38 and dpct <= -0.08:
            ab_s = 2
            reasons.append(f"حمله فروشنده + اسپایک حجم ({vratio:.1f}x)")
        elif vratio >= 1.5 and uwick >= 0.42 and cpos <= 0.45:
            ab_s = 1
            reasons.append(f"ریجکت نزولی با حجم بالا ({vratio:.1f}x)")

        r_i = max(range(47, 50), key=lambda i: sub[i]["high"])
        p_i = max(range(9, 45), key=lambda i: sub[i]["high"])
        if (
            r_i >= 48
            and cpos <= 0.50
            and sub[r_i]["high"] > sub[p_i]["high"]
            and cvd[-1] < cvd[p_i] - 0.80 * avg_ad
            and cvd[r_i] < cvd[p_i]
        ):
            cv_s = 2
            reasons.append("واگرایی نزولی قطعی Spot CVD")

        lvls = [
            n for n, v in [("VAH", vah), ("POC", poc), ("VWAP", vwap)]
            if curr["high"] >= v - 0.30 * atr and curr["close"] <= v + 0.15 * atr
        ]
        if lvls:
            lv_p = 1
            reasons.append(f"مقاومت روی سطح {'/'.join(lvls)}")
        sl_px = curr["high"] + 0.45 * atr

    c_score = sw48 + ab_s + cv_s + lv_p
    if not ((ab_s == 2 or cv_s == 2) and (sw48 == 2 or lv_p == 1) and c_score >= MIN_CANDLE_SCORE):
        return None

    entry = curr["close"]
    risk = (entry - sl_px) if direction == "LONG" else (sl_px - entry)
    if risk < 0.6 * atr:
        risk = 0.6 * atr
        sl_px = (entry - risk) if direction == "LONG" else (entry + risk)
    if risk > 3.5 * atr:
        return None

    tp15 = (entry + 1.5 * risk) if direction == "LONG" else (entry - 1.5 * risk)
    tp20 = (entry + 2.0 * risk) if direction == "LONG" else (entry - 2.0 * risk)

    return {
        "direction": direction,
        "candle_score": c_score,
        "score": c_score,
        "has_trio": (sw48 == 2 and ab_s == 2 and cv_s == 2),
        "reasons": reasons,
        "price": entry,
        "sl_px": sl_px,
        "tp15": tp15,
        "tp20": tp20,
        "risk_pct": (risk / entry) * 100.0,
        "vol_ratio": vratio,
        "delta_pct": dpct * 100.0,
        "poc": poc,
        "vwap": vwap,
    }


def apply_confirmations(cand, ob_info, hl_ctx, hl_mult):
    d = cand["direction"]
    reasons = list(cand["reasons"])
    score = cand["score"]

    ob_note = "نامشخص"
    if ob_info:
        _, _, ratio = ob_info
        inv = (1.0 / ratio) if ratio > 0 else 0.0
        ob_note = f"Bid/Ask: {ratio:.2f}x"
        if d == "LONG" and ratio >= 1.45:
            score += 1
            reasons.append(f"برتری دیوار خرید اسپات ({ratio:.2f}x)")
        elif d == "SHORT" and inv >= 1.45:
            score += 1
            reasons.append(f"برتری دیوار فروش اسپات ({inv:.2f}x)")

    hl_note = "در دسترس نیست"
    if hl_ctx:
        try:
            f8 = float(hl_ctx.get("funding", 0.0)) * 8.0 * 100.0
            raw_mk = float(hl_ctx.get("markPx", 0.0))
            mk = raw_mk / hl_mult
            oi_usd = (float(hl_ctx.get("openInterest", 0.0)) / 2.0) * raw_mk
            px = cand["price"]
            prem = ((mk - px) / px) * 100.0 if px > 0 else 0.0
            hl_note = f"OI {fmt_usd(oi_usd)} | F8h: {f8:+.3f}%"

            if d == "LONG" and (f8 <= -0.005 or prem <= -0.05):
                score += 1
                reasons.append(f"فشار شورت فیوچرز (Funding {f8:+.3f}%)")
            elif d == "SHORT" and (f8 >= 0.025 or prem >= 0.05):
                score += 1
                reasons.append(f"اشباع لانگ فیوچرز (Funding {f8:+.3f}%)")
        except Exception:
            pass

    cand["score"] = score
    cand["reasons"] = reasons
    cand["ob_note"] = ob_note
    cand["hl_note"] = hl_note
    return cand


def send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            r = requests.post(
                url,
                json=payload,
                headers={"Connection": "close"},
                timeout=(10, 25),
            )
            if r.status_code == 200:
                print("Telegram sent OK.")
                return
            print(f"Telegram HTTP {r.status_code}: {r.text}", file=sys.stderr)
            if 400 <= r.status_code < 500:
                return
        except Exception as e:
            print(f"Telegram try {attempt + 1}/3: {e}", file=sys.stderr)
        time.sleep(2 * (attempt + 1))


def format_alert(symbol: str, c: dict) -> str:
    d = c["direction"]
    dir_fa = "📈 لانگ (خرید)" if d == "LONG" else "📉 شورت (فروش)"
    dir_dot = "🟢" if d == "LONG" else "🔴"

    if c["has_trio"] or c["score"] >= 7:
        header = "💎💎💎 <b>الگوی الماس 1h (درجه ۱)</b>"
        border = "━━━━━━━━━━━━━━━━━━━━"
    elif c["score"] >= 6:
        header = f"🔥 <b>سیگنال ممتاز 1h {dir_dot}</b>"
        border = "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈"
    else:
        header = f"{dir_dot} <b>سیگنال تاییدشده 1h (مدل M5)</b>"
        border = "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈"

    trio_tag = " ⭐ <b>[سه‌گانه کامل]</b>" if c["has_trio"] else ""

    if d == "SHORT":
        note = "👑 <i>تاییدشده: شورت‌های 1h این مدل بهترین عملکرد تاریخی را داشته‌اند (+0.32R).</i>"
    else:
        note = "⚠️ <i>احتیاط در لانگ: در ۹۰ روز اخیر لانگ‌ها ضعیف‌تر بوده‌اند؛ حمایت 4h/Daily را چک کنید.</i>"

    lines = [
        header,
        border,
        f"🪙 ارز: <b>#{html.escape(symbol)}</b> | جهت: <b>{dir_fa}</b>",
        f"⏱ تایم: <b>1h</b>{trio_tag} | امتیاز: <b>{c['score']}/9</b> (کندلی {c['candle_score']}/7)",
        "",
        f"💰 قیمت ورود: <code>{fmt_price(c['price'])}</code>",
        f"🛑 حد ضرر مدل: <code>{fmt_price(c['sl_px'])}</code> (فاصله {c['risk_pct']:.2f}%)",
        f"🎯 تارگت ۱ (1.5R): <code>{fmt_price(c['tp15'])}</code>",
        f"🎯 تارگت ۲ (2.0R بهینه): <code>{fmt_price(c['tp20'])}</code>",
        "",
        "<b>✅ آیتم‌های تیک‌خورده:</b>",
    ]
    for r in c["reasons"]:
        lines.append(f"• {html.escape(r)}")

    lines.extend([
        "",
        f"📊 حجم 1h: <b>{c['vol_ratio']:.1f}x</b> | دلتا: <b>{c['delta_pct']:+.1f}%</b>",
        f"🧭 POC: <code>{fmt_price(c['poc'])}</code> | VWAP: <code>{fmt_price(c['vwap'])}</code>",
        f"📚 اردربوک: {html.escape(c['ob_note'])} | {html.escape(c['hl_note'])}",
        "",
        note,
    ])
    return "\n".join(lines)


def main():
    token = os.environ.get("TG_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TG_CHAT_ID", "").strip()
    mode = os.environ.get("RUN_MODE", "scan").strip().lower()

    if not token or not chat_id:
        print("ERROR: Missing TG_BOT_TOKEN or TG_CHAT_ID", file=sys.stderr)
        sys.exit(1)

    now_ms = int(time.time() * 1000)
    hl_ctxs = fetch_hl_contexts()

    kmap = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {
            ex.submit(fetch_klines, sym, TIMEFRAME, 160): (sym, hl_sym, mult)
            for sym, hl_sym, mult in COINS
        }
        for fut in as_completed(futs):
            sym, _, _ = futs[fut]
            kmap[sym] = fut.result()

    cands, near = [], []
    for sym, hl_sym, mult in COINS:
        raw = kmap.get(sym, [])
        closed = [c for c in raw if c["close_time"] < now_ms]
        if len(closed) < 60:
            continue

        age = now_ms - closed[-1]["close_time"]
        if mode != "test" and not (0 <= age <= FRESH_WINDOW_MS):
            continue

        atr = calc_atr(closed, 14)
        poc, vah, val = calc_vp(closed, 32)
        vwap = calc_vwap(closed, now_ms)

        found_any = False
        for d in ("LONG", "SHORT"):
            res = evaluate_m5(d, closed, atr, poc, vah, val, vwap)
            if res:
                res["symbol"] = sym
                res["hl_sym"] = hl_sym
                res["hl_mult"] = mult
                cands.append(res)
                found_any = True

        if mode == "test" and not found_any:
            cur = closed[-1]
            av = sum(c["qvol"] for c in closed[-21:-1]) / 20.0
            near.append((cur["qvol"] / max(av, 1.0), sym, cur["close"]))

    ob_map = {}
    syms = sorted({c["symbol"] for c in cands})
    if syms:
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(fetch_spot_ob, s): s for s in syms}
            for fut in as_completed(futs):
                ob_map[futs[fut]] = fut.result()

    sent = 0
    for c in cands:
        sym = c["symbol"]
        fin = apply_confirmations(
            c, ob_map.get(sym), hl_ctxs.get(c["hl_sym"]), c["hl_mult"]
        )
        send_telegram(token, chat_id, format_alert(sym, fin))
        sent += 1
        time.sleep(0.35)

    if mode == "test":
        utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        b_ok = len(kmap.get("BTCUSDT", [])) > 0
        h_ok = "BTC" in hl_ctxs
        near.sort(reverse=True)
        top = "\n".join(
            f"• {s} (1h): {v:.2f}x | <code>{fmt_price(p)}</code>"
            for v, s, p in near[:3]
        )
        msg = (
            "✅ <b>اسکنر نهایی M5 (تایم 1h | امتیاز ۵+) فعال شد!</b>\n"
            f"🕒 زمان: <code>{utc}</code>\n"
            f"📡 اسپات بایننس: {'متصل ✅' if b_ok else 'خطا ❌'}\n"
            f"📡 فیوچرز هایپرلیکوئید: {'متصل ✅' if h_ok else 'خطا ❌'}\n"
            f"🎯 سیگنال‌های فعلی: <b>{sent}</b>\n\n"
            f"<b>پرحجم‌ترین کندل‌های 1h اخیر:</b>\n{top or 'ندارد'}"
        )
        send_telegram(token, chat_id, msg)

    print(
        f"Done | mode={mode} | sent={sent} | hl={len(hl_ctxs)}"
    )


if __name__ == "__main__":
    main()

# --- END OF FILE ---
