import os
import sys
import time
import socket
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import requests
import urllib3.util.connection as urllib3_cn

urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

BINANCE_BASE = "https://data-api.binance.vision"

COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "SUIUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT",
    "LTCUSDT", "NEARUSDT", "ARBUSDT", "APTUSDT", "PEPEUSDT",
]

TF_BARS = {
    "15m": 8800,
    "30m": 4450,
    "1h": 2280,
    "4h": 650,
}

MAX_HOLD_BARS = 32
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "OrderFlow-Backtest-V2/1.0"})


def fetch_history(symbol: str, tf: str, target: int) -> list:
    url = f"{BINANCE_BASE}/api/v3/klines"
    end_ts = int(time.time() * 1000) - 1000
    rows = {}

    while len(rows) < target:
        got = None
        for _ in range(3):
            try:
                r = SESSION.get(
                    url,
                    params={
                        "symbol": symbol,
                        "interval": tf,
                        "limit": 1000,
                        "endTime": end_ts,
                    },
                    timeout=12,
                )
                if r.status_code == 200:
                    got = r.json()
                    break
            except Exception:
                pass
            time.sleep(0.5)

        if not got:
            break

        for k in got:
            ot = int(k[0])
            qv = float(k[7])
            bqv = float(k[10])
            sqv = max(qv - bqv, 0.0)
            rows[ot] = {
                "ot": ot,
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "ct": int(k[6]),
                "qv": qv,
                "dl": bqv - sqv,
            }

        oldest = int(got[0][0])
        if len(got) < 1000 or oldest >= end_ts:
            break
        end_ts = oldest - 1

    now_ms = int(time.time() * 1000)
    out = [rows[t] for t in sorted(rows) if rows[t]["ct"] < now_ms]
    return out[-target:]


def calc_ema(candles: list, period: int) -> list:
    k = 2.0 / (period + 1)
    out = [candles[0]["c"]]
    for i in range(1, len(candles)):
        out.append(candles[i]["c"] * k + out[-1] * (1.0 - k))
    return out


def calc_vp_slice(candles: list, end_i: int, bins: int = 32):
    sub = candles[end_i - 47:end_i + 1]
    p_min = min(c["l"] for c in sub)
    p_max = max(c["h"] for c in sub)
    if p_max <= p_min:
        return p_min, p_min, p_min

    step = (p_max - p_min) / bins
    bvols = [0.0] * bins
    for c in sub:
        lo_i = max(0, min(bins - 1, int((c["l"] - p_min) / step)))
        hi_i = max(0, min(bins - 1, int((c["h"] - p_min) / step)))
        share = c["qv"] / (hi_i - lo_i + 1)
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


def calc_vwap_at(candles: list, i: int, tf: str) -> float:
    if tf == "4h":
        start = max(0, i - 41)
    else:
        day0 = (candles[i]["ot"] // 86_400_000) * 86_400_000
        start = i
        while start > 0 and candles[start - 1]["ot"] >= day0:
            start -= 1
        if (i - start + 1) < 4:
            start = max(0, i - 23)

    num = den = 0.0
    for c in candles[start:i + 1]:
        tp = (c["h"] + c["l"] + c["c"]) / 3.0
        num += tp * c["qv"]
        den += c["qv"]
    return (num / den) if den > 0 else candles[i]["c"]


def sim_trade(candles, i, d, entry, sl, rr):
    risk = (entry - sl) if d == "LONG" else (sl - entry)
    tp = (entry + rr * risk) if d == "LONG" else (entry - rr * risk)
    end_j = min(len(candles) - 1, i + MAX_HOLD_BARS)

    for j in range(i + 1, end_j + 1):
        bar = candles[j]
        if d == "LONG":
            hit_sl = bar["l"] <= sl
            hit_tp = bar["h"] >= tp
        else:
            hit_sl = bar["h"] >= sl
            hit_tp = bar["l"] <= tp

        if hit_sl:
            return "LOSS", -1.0, j
        if hit_tp:
            return "WIN", rr, j

    end_px = candles[end_j]["c"]
    r_pnl = ((end_px - entry) if d == "LONG" else (entry - end_px)) / risk
    return "TIME", r_pnl, end_j


def record_trade(buckets, model_name, busy, sym, tf, d, i, candles, entry, atr, sl_mult):
    if i <= busy[(model_name, d)]:
        return
    sl = (candles[i]["l"] - sl_mult * atr) if d == "LONG" else (candles[i]["h"] + sl_mult * atr)
    risk = (entry - sl) if d == "LONG" else (sl - entry)
    if risk < 0.6 * atr:
        risk = 0.6 * atr
        sl = (entry - risk) if d == "LONG" else (entry + risk)
    if risk > 3.5 * atr:
        return

    o15, r15, _ = sim_trade(candles, i, d, entry, sl, 1.5)
    o20, r20, ex_j = sim_trade(candles, i, d, entry, sl, 2.0)
    busy[(model_name, d)] = ex_j + 1

    buckets[model_name].append({
        "sym": sym, "tf": tf, "d": d,
        "o15": o15, "r15": r15,
        "o20": o20, "r20": r20,
    })


def test_series(sym: str, tf: str, candles: list) -> dict:
    models = ["M1_TREND", "M2_STRICT5", "M3_TREND_15_1H", "M4_EXTREME", "M5_1H_PURE"]
    buckets = {m: [] for m in models}
    n = len(candles)
    if n < 120:
        return buckets

    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    p_tr = [0.0] * (n + 1)
    p_qv = [0.0] * (n + 1)
    p_dl = [0.0] * (n + 1)
    p_ad = [0.0] * (n + 1)
    for i in range(n):
        p_tr[i + 1] = p_tr[i] + trs[i]
        p_qv[i + 1] = p_qv[i] + candles[i]["qv"]
        p_dl[i + 1] = p_dl[i] + candles[i]["dl"]
        p_ad[i + 1] = p_ad[i] + abs(candles[i]["dl"])

    ema20 = calc_ema(candles, 20)
    ema50 = calc_ema(candles, 50)
    ema100 = calc_ema(candles, 100)

    busy = {(m, d): -1 for m in models for d in ("LONG", "SHORT")}

    for i in range(100, n - 5):
        cur = candles[i]
        crange = cur["h"] - cur["l"]
        atr = max((p_tr[i + 1] - p_tr[i - 13]) / 14.0, cur["c"] * 0.0005)
        if crange < 0.50 * atr or cur["qv"] <= 0:
            continue

        cpos = (cur["c"] - cur["l"]) / crange
        uwick = (cur["h"] - max(cur["o"], cur["c"])) / crange
        lwick = (min(cur["o"], cur["c"]) - cur["l"]) / crange
        avg_qv = (p_qv[i] - p_qv[i - 20]) / 20.0
        vratio = cur["qv"] / max(avg_qv, 1.0)
        dpct = cur["dl"] / cur["qv"]
        avg_ad = (p_ad[i] - p_ad[i - 49]) / 49.0

        p20_lo = min(candles[k]["l"] for k in range(i - 20, i))
        p20_hi = max(candles[k]["h"] for k in range(i - 20, i))
        p48_lo = min(candles[k]["l"] for k in range(i - 48, i))
        p48_hi = max(candles[k]["h"] for k in range(i - 48, i))

        vp_cache = None

        for d in ("LONG", "SHORT"):
            sw20 = sw48 = ab_s = cv_s = lv_p = 0

            if d == "LONG":
                if cur["l"] < p20_lo - 0.05 * atr and cur["c"] > p20_lo and cpos >= 0.50:
                    sw20 = 2
                if cur["l"] < p48_lo - 0.10 * atr and cur["c"] > p48_lo and cpos >= 0.55:
                    sw48 = 2

                if vratio >= 1.9 and lwick >= 0.42 and cpos >= 0.52 and dpct <= -0.06:
                    ab_s = 2
                elif vratio >= 1.9 and lwick >= 0.35 and cpos >= 0.62 and dpct >= 0.08:
                    ab_s = 2
                elif vratio >= 1.5 and lwick >= 0.42 and cpos >= 0.55:
                    ab_s = 1

                r_i = min(range(i - 2, i + 1), key=lambda k: candles[k]["l"])
                p_i = min(range(i - 40, i - 5), key=lambda k: candles[k]["l"])
                if (
                    r_i >= i - 1
                    and cpos >= 0.50
                    and candles[r_i]["l"] < candles[p_i]["l"]
                    and p_dl[i + 1] > p_dl[p_i + 1] + 0.80 * avg_ad
                    and p_dl[r_i + 1] > p_dl[p_i + 1]
                ):
                    cv_s = 2

                trend_ok = (cur["c"] > ema100[i] and ema50[i] > ema50[i - 6])
                stretched = (cur["l"] < ema50[i] - 1.5 * atr)
            else:
                if cur["h"] > p20_hi + 0.05 * atr and cur["c"] < p20_hi and cpos <= 0.50:
                    sw20 = 2
                if cur["h"] > p48_hi + 0.10 * atr and cur["c"] < p48_hi and cpos <= 0.45:
                    sw48 = 2

                if vratio >= 1.9 and uwick >= 0.42 and cpos <= 0.48 and dpct >= 0.06:
                    ab_s = 2
                elif vratio >= 1.9 and uwick >= 0.35 and cpos <= 0.38 and dpct <= -0.08:
                    ab_s = 2
                elif vratio >= 1.5 and uwick >= 0.42 and cpos <= 0.45:
                    ab_s = 1

                r_i = max(range(i - 2, i + 1), key=lambda k: candles[k]["h"])
                p_i = max(range(i - 40, i - 5), key=lambda k: candles[k]["h"])
                if (
                    r_i >= i - 1
                    and cpos <= 0.50
                    and candles[r_i]["h"] > candles[p_i]["h"]
                    and p_dl[i + 1] < p_dl[p_i + 1] - 0.80 * avg_ad
                    and p_dl[r_i + 1] < p_dl[p_i + 1]
                ):
                    cv_s = 2

                trend_ok = (cur["c"] < ema100[i] and ema50[i] < ema50[i - 6])
                stretched = (cur["h"] > ema50[i] + 1.5 * atr)

            if not (ab_s >= 1 or cv_s == 2 or sw48 == 2):
                continue

            if vp_cache is None:
                poc, vah, val = calc_vp_slice(candles, i, 32)
                vwap = calc_vwap_at(candles, i, tf)
                vp_cache = (poc, vah, val, vwap)
            else:
                poc, vah, val, vwap = vp_cache

            if d == "LONG":
                if any(cur["l"] <= v + 0.30 * atr and cur["c"] >= v - 0.15 * atr for v in (val, poc, vwap)):
                    lv_p = 1
            else:
                if any(cur["h"] >= v - 0.30 * atr and cur["c"] <= v + 0.15 * atr for v in (vah, poc, vwap)):
                    lv_p = 1

            strict_score = sw48 + ab_s + cv_s + lv_p
            entry = cur["c"]

            # M1: هم‌جهت روند EMA50/100 + محرک قوی اردرفلو
            if trend_ok and (ab_s == 2 or cv_s == 2) and (sw20 == 2 or lv_p == 1):
                record_trade(buckets, "M1_TREND", busy, sym, tf, d, i, candles, entry, atr, 0.35)

            # M2: الگوی سخت‌گیرانه (امتیاز سخت 5+ از 7)
            if (ab_s == 2 or cv_s == 2) and (sw48 == 2 or lv_p == 1) and strict_score >= 5:
                record_trade(buckets, "M2_STRICT5", busy, sym, tf, d, i, candles, entry, atr, 0.40)

            # M3: هم‌جهت روند + فیلتر سخت فقط روی 15m و 1h
            if tf in ("15m", "1h") and trend_ok and (ab_s == 2 or cv_s == 2) and (sw48 == 2 or lv_p == 1):
                record_trade(buckets, "M3_TREND_15_1H", busy, sym, tf, d, i, candles, entry, atr, 0.40)

            # M4: برگشت ضد روند فقط در کشیدگی شدید از EMA50 + Sweep48
            if stretched and sw48 == 2 and (ab_s == 2 or cv_s == 2):
                record_trade(buckets, "M4_EXTREME", busy, sym, tf, d, i, candles, entry, atr, 0.45)

            # M5: تایم‌فریم 1h خالص با امتیاز سخت 4+ و استاپ 0.45 ATR
            if tf == "1h" and (ab_s == 2 or cv_s == 2) and (sw48 == 2 or lv_p == 1) and strict_score >= 4:
                record_trade(buckets, "M5_1H_PURE", busy, sym, tf, d, i, candles, entry, atr, 0.45)

    return buckets


def fmt_stat(items: list, title: str) -> str:
    n = len(items)
    if n == 0:
        return f"• <b>{title}</b>: 0 سیگنال"
    w15 = sum(1 for x in items if x["o15"] == "WIN")
    l15 = sum(1 for x in items if x["o15"] == "LOSS")
    wr15 = (w15 / max(w15 + l15, 1)) * 100.0
    e15 = sum(x["r15"] for x in items) / n

    w20 = sum(1 for x in items if x["o20"] == "WIN")
    l20 = sum(1 for x in items if x["o20"] == "LOSS")
    wr20 = (w20 / max(w20 + l20, 1)) * 100.0
    e20 = sum(x["r20"] for x in items) / n

    per_day = n / 90.0
    return (
        f"• <b>{title}</b> ({n} تا | ~{per_day:.1f}/روز):\n"
        f"   1.5R : برد <b>{wr15:.1f}%</b> | میانگین <b>{e15:+.2f}R</b>\n"
        f"   2.0R : برد <b>{wr20:.1f}%</b> | میانگین <b>{e20:+.2f}R</b>"
    )


def send_telegram(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(
                url,
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
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
            print(f"TG try {attempt + 1}/3: {e}", file=sys.stderr)
        time.sleep(2 * (attempt + 1))


def main():
    t0 = time.time()
    tasks = [(sym, tf, bars) for sym in COINS for tf, bars in TF_BARS.items()]
    all_b = {
        "M1_TREND": [],
        "M2_STRICT5": [],
        "M3_TREND_15_1H": [],
        "M4_EXTREME": [],
        "M5_1H_PURE": [],
    }

    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_history, s, tf, b): (s, tf) for s, tf, b in tasks}
        for fut in futs:
            s, tf = futs[fut]
            res = test_series(s, tf, fut.result())
            for k in all_b:
                all_b[k].extend(res[k])

    el = time.time() - t0
    utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "🧪 <b>مسابقه ۵ مدل بهینه‌شده (۹۰ روز اخیر | ۱۵ ارز)</b>",
        f"🕒 زمان: <code>{utc}</code> ({el:.1f}s)",
        "<i>مدل برنده باید بعد از کسر کارمزد (~0.06R) هم کاملاً مثبت بماند:</i>",
        "",
        fmt_stat(all_b["M1_TREND"], "M1: هم‌جهت روند + اردر فلو"),
        fmt_stat(all_b["M2_STRICT5"], "M2: سخت‌گیرانه ۵+ (Sweep48 + جذب 1.9x)"),
        fmt_stat(all_b["M3_TREND_15_1H"], "M3: هم‌جهت روند + Sweep48 (فقط 15m/1h)"),
        fmt_stat(all_b["M4_EXTREME"], "M4: برگشت ضد روند (فقط کشیدگی شدید)"),
        fmt_stat(all_b["M5_1H_PURE"], "M5: خالص 1h سخت‌گیرانه"),
    ]

    msg = "\n".join(lines)
    print(msg)

    token = os.environ.get("TG_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TG_CHAT_ID", "").strip()
    if token and chat_id:
        send_telegram(token, chat_id, msg)


if __name__ == "__main__":
    main()

# --- END OF FILE ---
