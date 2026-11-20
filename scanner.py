import os
import sys
import time
import html
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ==========================================
# تنظیمات اصلی (در صورت نیاز قابل تغییر)
# ==========================================
BINANCE_SPOT_BASE = "https://data-api.binance.vision"
HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"

# (نماد بایننس اسپات, نماد هایپرلیکوئید, ضریب قیمت هایپرلیکوئید نسبت به اسپات)
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

TIMEFRAMES = ["15m", "30m", "1h", "4h"]
FRESH_WINDOW_MS = 14 * 60 * 1000  # فقط کندل‌هایی که در 14 دقیقه اخیر بسته شده‌اند
MIN_FINAL_SCORE = 5               # حداقل امتیاز برای سیگنال قوی (از 9)
MAX_WORKERS = 8

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "OrderFlow-Scanner/1.0"})


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


def fetch_hyperliquid_contexts() -> dict:
    """دریافت OI، Funding Rate و Mark Price کل ارزهای Hyperliquid با یک درخواست."""
    for attempt in range(3):
        try:
            r = SESSION.post(
                HYPERLIQUID_INFO,
                json={"type": "metaAndAssetCtxs"},
                timeout=12,
            )
            if r.status_code == 200:
                data = r.json()
                universe = data[0].get("universe", [])
                ctxs = data[1]
                result = {}
                for meta, ctx in zip(universe, ctxs):
                    name = meta.get("name")
                    if name:
                        result[name] = ctx
                return result
        except Exception:
            pass
        time.sleep(1.0 * (attempt + 1))
    return {}


def fetch_binance_klines(symbol: str, tf: str, limit: int = 100) -> list:
    """دریافت کندل‌های اسپات بایننس همراه با حجم خریدار مهاجم (Taker Buy Volume)."""
    url = f"{BINANCE_SPOT_BASE}/api/v3/klines"
    for attempt in range(2):
        try:
            r = SESSION.get(
                url,
                params={"symbol": symbol, "interval": tf, "limit": limit},
                timeout=10,
            )
            if r.status_code == 200:
                raw = r.json()
                candles = []
                for k in raw:
                    qvol = float(k[7])
                    buy_qvol = float(k[10])
                    sell_qvol = max(qvol - buy_qvol, 0.0)
                    candles.append({
                        "open_time": int(k[0]),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "vol": float(k[5]),
                        "close_time": int(k[6]),
                        "qvol": qvol,
                        "buy_qvol": buy_qvol,
                        "delta_usdt": buy_qvol - sell_qvol,
                    })
                return candles
        except Exception:
            pass
        time.sleep(0.5)
    return []


def fetch_spot_orderbook_imbalance(symbol: str) -> tuple[float, float, float] | None:
    """محاسبه برتری خریدار یا فروشنده در عمق 0.5% اردربوک اسپات."""
    url = f"{BINANCE_SPOT_BASE}/api/v3/depth"
    try:
        r = SESSION.get(url, params={"symbol": symbol, "limit": 500}, timeout=10)
        if r.status_code != 200:
            return None
        book = r.json()
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        bid_floor = best_bid * 0.995   # تا 0.5% پایین‌تر
        ask_ceil = best_ask * 1.005    # تا 0.5% بالاتر

        bid_usd = sum(float(p) * float(q) for p, q in bids if float(p) >= bid_floor)
        ask_usd = sum(float(p) * float(q) for p, q in asks if float(p) <= ask_ceil)
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
    if len(trs) < period:
        atr = sum(trs) / max(len(trs), 1)
    else:
        atr = sum(trs[-period:]) / period
    return max(atr, candles[-1]["close"] * 0.0005)


def calc_volume_profile(candles: list, bins: int = 32) -> tuple[float, float, float]:
    """محاسبه POC، VAH و VAL روی 48 کندل اخیر."""
    sub = candles[-48:]
    p_min = min(c["low"] for c in sub)
    p_max = max(c["high"] for c in sub)
    if p_max <= p_min:
        mid = p_min
        return mid, mid, mid

    step = (p_max - p_min) / bins
    bin_vols = [0.0] * bins

    for c in sub:
        lo_i = max(0, min(bins - 1, int((c["low"] - p_min) / step)))
        hi_i = max(0, min(bins - 1, int((c["high"] - p_min) / step)))
        span = hi_i - lo_i + 1
        share = c["qvol"] / span
        for idx in range(lo_i, hi_i + 1):
            bin_vols[idx] += share

    total_vol = sum(bin_vols)
    poc_idx = max(range(bins), key=lambda i: bin_vols[i])
    poc = p_min + (poc_idx + 0.5) * step

    target = total_vol * 0.70
    acc = bin_vols[poc_idx]
    left = poc_idx
    right = poc_idx

    while acc < target and (left > 0 or right < bins - 1):
        v_left = bin_vols[left - 1] if left > 0 else -1.0
        v_right = bin_vols[right + 1] if right < bins - 1 else -1.0
        if v_left >= v_right and left > 0:
            left -= 1
            acc += bin_vols[left]
        elif right < bins - 1:
            right += 1
            acc += bin_vols[right]
        else:
            break

    val = p_min + left * step
    vah = p_min + (right + 1) * step
    return poc, vah, val


def calc_vwap(candles: list, tf: str, now_ms: int) -> float:
    if tf == "4h":
        sub = candles[-42:]
    else:
        today_start = (now_ms // 86_400_000) * 86_400_000
        sub = [c for c in candles if c["open_time"] >= today_start]
        if len(sub) < 4:
            sub = candles[-24:]

    num = 0.0
    den = 0.0
    for c in sub:
        tp = (c["high"] + c["low"] + c["close"]) / 3.0
        num += tp * c["qvol"]
        den += c["qvol"]
    return (num / den) if den > 0 else candles[-1]["close"]


def evaluate_direction(
    direction: str,
    closed: list,
    atr: float,
    poc: float,
    vah: float,
    val: float,
    vwap: float,
) -> dict | None:
    """ارزیابی اولیه لایه‌های کندلی، مکان، جذب و واگرایی CVD (پیش از چک کردن اردربوک)."""
    curr = closed[-1]
    crange = curr["high"] - curr["low"]
    if crange < 0.45 * atr or curr["qvol"] <= 0:
        return None

    close_pos = (curr["close"] - curr["low"]) / crange
    upper_wick = curr["high"] - max(curr["open"], curr["close"])
    lower_wick = min(curr["open"], curr["close"]) - curr["low"]
    upper_wick_ratio = upper_wick / crange
    lower_wick_ratio = lower_wick / crange

    avg_qvol = sum(c["qvol"] for c in closed[-21:-1]) / 20.0
    vol_ratio = curr["qvol"] / max(avg_qvol, 1.0)
    delta_pct = curr["delta_usdt"] / curr["qvol"]

    prior_20_low = min(c["low"] for c in closed[-21:-1])
    prior_20_high = max(c["high"] for c in closed[-21:-1])

    # ساخت سری CVD روی 50 کندل اخیر
    sub = closed[-50:]
    cvd = []
    running = 0.0
    for c in sub:
        running += c["delta_usdt"]
        cvd.append(running)
    avg_abs_delta = sum(abs(c["delta_usdt"]) for c in sub[:-1]) / max(len(sub) - 1, 1)

    reasons = []
    sweep_pts = 0
    abs_pts = 0
    cvd_pts = 0
    level_pts = 0

    if direction == "LONG":
        # 1) Liquidity Sweep
        if (
            curr["low"] < prior_20_low - 0.05 * atr
            and curr["close"] > prior_20_low
            and close_pos >= 0.45
        ):
            sweep_pts = 2
            reasons.append("شکار لیکوئیدیتی کف (Liquidity Sweep)")

        # 2) Absorption / Reversal Volume
        if vol_ratio >= 1.7 and lower_wick_ratio >= 0.42 and close_pos >= 0.50 and delta_pct <= -0.06:
            abs_pts = 2
            reasons.append(f"جذب لیمیت خرید + شدوی برگشتی (حجم {vol_ratio:.1f}x)")
        elif vol_ratio >= 1.7 and lower_wick_ratio >= 0.32 and close_pos >= 0.62 and delta_pct >= 0.08:
            abs_pts = 2
            reasons.append(f"حمله خریدار + اسپایک حجم (حجم {vol_ratio:.1f}x)")
        elif vol_ratio >= 1.35 and lower_wick_ratio >= 0.45 and close_pos >= 0.55:
            abs_pts = 1
            reasons.append(f"ریجکت صعودی با حجم بالا ({vol_ratio:.1f}x)")

        # 3) Spot CVD Divergence
        recent_low_i = min(range(47, 50), key=lambda i: sub[i]["low"])
        prior_low_i = min(range(15, 45), key=lambda i: sub[i]["low"])
        if (
            recent_low_i >= 48
            and close_pos >= 0.45
            and sub[recent_low_i]["low"] <= sub[prior_low_i]["low"] + 0.15 * atr
            and cvd[-1] > cvd[prior_low_i] + 0.40 * avg_abs_delta
            and cvd[recent_low_i] > cvd[prior_low_i]
        ):
            cvd_pts = 2
            reasons.append("واگرایی صعودی Spot CVD")

        # 4) Key Volume Level
        matched_levels = []
        for name, lvl in [("VAL", val), ("POC", poc), ("VWAP", vwap)]:
            if curr["low"] <= lvl + 0.30 * atr and curr["close"] >= lvl - 0.15 * atr:
                matched_levels.append(name)
        if matched_levels:
            level_pts = 1
            reasons.append(f"حمایت روی سطح {'/'.join(matched_levels)}")

    else:  # SHORT
        # 1) Liquidity Sweep
        if (
            curr["high"] > prior_20_high + 0.05 * atr
            and curr["close"] < prior_20_high
            and close_pos <= 0.55
        ):
            sweep_pts = 2
            reasons.append("شکار لیکوئیدیتی سقف (Liquidity Sweep)")

        # 2) Absorption / Reversal Volume
        if vol_ratio >= 1.7 and upper_wick_ratio >= 0.42 and close_pos <= 0.50 and delta_pct >= 0.06:
            abs_pts = 2
            reasons.append(f"جذب لیمیت فروش + شدوی برگشتی (حجم {vol_ratio:.1f}x)")
        elif vol_ratio >= 1.7 and upper_wick_ratio >= 0.32 and close_pos <= 0.38 and delta_pct <= -0.08:
            abs_pts = 2
            reasons.append(f"حمله فروشنده + اسپایک حجم (حجم {vol_ratio:.1f}x)")
        elif vol_ratio >= 1.35 and upper_wick_ratio >= 0.45 and close_pos <= 0.45:
            abs_pts = 1
            reasons.append(f"ریجکت نزولی با حجم بالا ({vol_ratio:.1f}x)")

        # 3) Spot CVD Divergence
        recent_high_i = max(range(47, 50), key=lambda i: sub[i]["high"])
        prior_high_i = max(range(15, 45), key=lambda i: sub[i]["high"])
        if (
            recent_high_i >= 48
            and close_pos <= 0.55
            and sub[recent_high_i]["high"] >= sub[prior_high_i]["high"] - 0.15 * atr
            and cvd[-1] < cvd[prior_high_i] - 0.40 * avg_abs_delta
            and cvd[recent_high_i] < cvd[prior_high_i]
        ):
            cvd_pts = 2
            reasons.append("واگرایی نزولی Spot CVD")

        # 4) Key Volume Level
        matched_levels = []
        for name, lvl in [("VAH", vah), ("POC", poc), ("VWAP", vwap)]:
            if curr["high"] >= lvl - 0.30 * atr and curr["close"] <= lvl + 0.15 * atr:
                matched_levels.append(name)
        if matched_levels:
            level_pts = 1
            reasons.append(f"مقاومت روی سطح {'/'.join(matched_levels)}")

    has_core_of = (abs_pts == 2) or (cvd_pts == 2)
    has_core_loc = (sweep_pts == 2) or (level_pts == 1)
    pre_score = sweep_pts + abs_pts + cvd_pts + level_pts

    if not (has_core_of and has_core_loc and pre_score >= 3):
        return None

    return {
        "direction": direction,
        "pre_score": pre_score,
        "score": pre_score,
        "reasons": reasons,
        "price": curr["close"],
        "vol_ratio": vol_ratio,
        "delta_pct": delta_pct * 100.0,
        "poc": poc,
        "vah": vah,
        "val": val,
        "vwap": vwap,
    }


def apply_realtime_confirmations(
    cand: dict,
    ob_info: tuple[float, float, float] | None,
    hl_ctx: dict | None,
    hl_mult: int,
) -> dict:
    """افزودن امتیاز اردربوک اسپات و داده‌های فیوچرز Hyperliquid."""
    direction = cand["direction"]
    reasons = list(cand["reasons"])
    score = cand["score"]

    ob_note = "نامشخص"
    if ob_info:
        bid_usd, ask_usd, ratio = ob_info
        inv_ratio = 1.0 / ratio if ratio > 0 else 0.0
        ob_note = f"Bid/Ask: {ratio:.2f}x"
        if direction == "LONG" and ratio >= 1.45:
            score += 1
            reasons.append(f"برتری دیوار خرید اسپات ({ratio:.2f}x)")
        elif direction == "SHORT" and inv_ratio >= 1.45:
            score += 1
            reasons.append(f"برتری دیوار فروش اسپات ({inv_ratio:.2f}x)")

    hl_note = "در دسترس نیست"
    if hl_ctx:
        try:
            funding_hr = float(hl_ctx.get("funding", 0.0))
            funding_8h_pct = funding_hr * 8.0 * 100.0
            mark_px = float(hl_ctx.get("markPx", 0.0)) / hl_mult
            oi_coins = float(hl_ctx.get("openInterest", 0.0))
            # Open interest در Hyperliquid مجموع لانگ+شورت است؛ تقسیم بر 2 برای یک سمت
            oi_usd = (oi_coins / 2.0) * float(hl_ctx.get("markPx", 0.0))
            prem_pct = ((mark_px - cand["price"]) / cand["price"]) * 100.0 if cand["price"] > 0 else 0.0
            hl_note = f"OI {fmt_usd(oi_usd)} | Funding 8h: {funding_8h_pct:+.3f}%"

            if direction == "LONG" and (funding_8h_pct <= -0.005 or prem_pct <= -0.05):
                score += 1
                reasons.append(f"فشار شورت/تخفیف فیوچرز (Funding {funding_8h_pct:+.3f}%)")
            elif direction == "SHORT" and (funding_8h_pct >= 0.025 or prem_pct >= 0.05):
                score += 1
                reasons.append(f"اشباع لانگ در فیوچرز (Funding {funding_8h_pct:+.3f}%)")
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
    r = SESSION.post(url, json=payload, timeout=12)
    if r.status_code != 200:
        print(f"Telegram error {r.status_code}: {r.text}", file=sys.stderr)


def format_alert_message(symbol: str, direction: str, items: list) -> str:
    # مرتب‌سازی بر اساس بالاترین امتیاز و تایم‌فریم بزرگ‌تر
    tf_rank = {"4h": 4, "1h": 3, "30m": 2, "15m": 1}
    items.sort(key=lambda x: (x["score"], tf_rank.get(x["tf"], 0)), reverse=True)
    best = items[0]
    max_score = best["score"]
    tfs_str = ", ".join(x["tf"] for x in items)

    is_ultra = max_score >= 7 or len(items) >= 2
    if direction == "LONG":
        badge = "🔥 <b>سوپر سیگنال لانگ (LONG)</b>" if is_ultra else "🟢 <b>سیگنال قوی لانگ (LONG)</b>"
    else:
        badge = "🔥 <b>سوپر سیگنال شورت (SHORT)</b>" if is_ultra else "🔴 <b>سیگنال قوی شورت (SHORT)</b>"

    mtf_tag = " [هم‌گرایی چند تایم‌فریم!]" if len(items) >= 2 else ""

    lines = [
        f"{badge} | <b>#{html.escape(symbol)}</b>",
        f"⏱ تایم‌فریم: <b>{tfs_str}</b>{mtf_tag} | امتیاز: <b>{max_score} / 9</b>",
        f"💰 قیمت اسپات: <code>{fmt_price(best['price'])}</code>",
        "",
        "<b>✅ تاییدیه‌های هم‌زمان:</b>",
    ]
    for r in best["reasons"]:
        lines.append(f"• {html.escape(r)}")

    lines.extend([
        "",
        "<b>📊 خلاصه اردر فلو:</b>",
        f"• حجم کندل ({best['tf']}): <b>{best['vol_ratio']:.1f}x</b> | دلتا: <b>{best['delta_pct']:+.1f}%</b>",
        f"• سطوح: POC <code>{fmt_price(best['poc'])}</code> | VWAP <code>{fmt_price(best['vwap'])}</code>",
        f"• محدوده ارزش: VAL <code>{fmt_price(best['val'])}</code> — VAH <code>{fmt_price(best['vah'])}</code>",
        f"• اردربوک اسپات: {html.escape(best['ob_note'])}",
        f"• فیوچرز (HL): {html.escape(best['hl_note'])}",
    ])
    return "\n".join(lines)


def main():
    bot_token = os.environ.get("TG_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TG_CHAT_ID", "").strip()
    run_mode = os.environ.get("RUN_MODE", "scan").strip().lower()

    if not bot_token or not chat_id:
        print("ERROR: TG_BOT_TOKEN or TG_CHAT_ID is missing!", file=sys.stderr)
        sys.exit(1)

    now_ms = int(time.time() * 1000)
    hl_contexts = fetch_hyperliquid_contexts()

    # دریافت هم‌زمان کندل‌های 15 ارز در 4 تایم‌فریم (60 درخواست سبک)
    tasks = []
    klines_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for sym, hl_sym, mult in COINS:
            for tf in TIMEFRAMES:
                fut = ex.submit(fetch_binance_klines, sym, tf, 100)
                tasks.append((fut, sym, hl_sym, mult, tf))

        for fut, sym, hl_sym, mult, tf in tasks:
            klines_map[(sym, tf)] = fut.result()

    # مرحله اول: ارزیابی کندل‌های تازه بسته‌شده
    candidates = []
    near_misses = []

    for sym, hl_sym, mult in COINS:
        for tf in TIMEFRAMES:
            raw = klines_map.get((sym, tf), [])
            closed = [c for c in raw if c["close_time"] < now_ms]
            if len(closed) < 55:
                continue

            age_ms = now_ms - closed[-1]["close_time"]
            if run_mode != "test" and not (0 <= age_ms <= FRESH_WINDOW_MS):
                continue

            atr = calc_atr(closed, 14)
            poc, vah, val = calc_volume_profile(closed, 32)
            vwap = calc_vwap(closed, tf, now_ms)

            for direction in ("LONG", "SHORT"):
                res = evaluate_direction(direction, closed, atr, poc, vah, val, vwap)
                if res:
                    res["symbol"] = sym
                    res["hl_sym"] = hl_sym
                    res["hl_mult"] = mult
                    res["tf"] = tf
                    candidates.append(res)
                elif run_mode == "test":
                    # برای گزارش تست، وضعیت کلی بازار را نگه می‌داریم
                    curr = closed[-1]
                    avg_v = sum(c["qvol"] for c in closed[-21:-1]) / 20.0
                    near_misses.append((curr["qvol"] / max(avg_v, 1.0), sym, tf, curr["close"]))

    # مرحله دوم: دریافت اردربوک لحظه‌ای فقط برای کاندیداهای مستعد
    unique_syms = sorted({c["symbol"] for c in candidates})
    ob_map = {}
    if unique_syms:
        with ThreadPoolExecutor(max_workers=4) as ex:
            ob_futs = {ex.submit(fetch_spot_orderbook_imbalance, sym): sym for sym in unique_syms}
            for fut in as_completed(ob_futs):
                sym = ob_futs[fut]
                ob_map[sym] = fut.result()

    # مرحله سوم: امتیازدهی نهایی و فیلتر سیگنال‌های قوی
    grouped_alerts = {}
    for cand in candidates:
        sym = cand["symbol"]
        final = apply_realtime_confirmations(
            cand,
            ob_map.get(sym),
            hl_contexts.get(cand["hl_sym"]),
            cand["hl_mult"],
        )
        if final["score"] >= MIN_FINAL_SCORE:
            key = (sym, final["direction"])
            grouped_alerts.setdefault(key, []).append(final)

    # ارسال پیام‌ها به تلگرام
    sent_count = 0
    for (sym, direction), items in grouped_alerts.items():
        msg = format_alert_message(sym, direction, items)
        send_telegram(bot_token, chat_id, msg)
        sent_count += 1
        time.sleep(0.35)

    # در حالت تست، حتی اگر سیگنال قوی نباشد یک گزارش سلامت به تلگرام می‌فرستد
    if run_mode == "test":
        utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        btc_ok = len(klines_map.get(("BTCUSDT", "15m"), [])) > 0
        hl_ok = "BTC" in hl_contexts
        near_misses.sort(reverse=True)
        top_vol = "\n".join(
            f"• {sym} ({tf}): حجم {vr:.2f}x | قیمت <code>{fmt_price(px)}</code>"
            for vr, sym, tf, px in near_misses[:3]
        )
        status_msg = (
            "✅ <b>تست اتصال اسکنر اردر فلو موفق بود!</b>\n"
            f"🕒 زمان سرور: <code>{utc_str}</code>\n"
            f"📡 دیتای اسپات بایننس: {'متصل ✅' if btc_ok else 'خطا ❌'}\n"
            f"📡 دیتای فیوچرز هایپرلیکوئید: {'متصل ✅' if hl_ok else 'خطا ❌'}\n"
            f"🎯 سیگنال‌های قوی یافت‌شده در این لحظه: <b>{sent_count}</b>\n\n"
            f"<b>پرحجم‌ترین کندل‌های اخیر (صرفاً جهت تست):</b>\n{top_vol or 'موردی ثبت نشد'}"
        )
        send_telegram(bot_token, chat_id, status_msg)

    print(
        f"Done | mode={run_mode} | candidates={len(candidates)} | alerts_sent={sent_count} | hl_coins={len(hl_contexts)}"
    )


if __name__ == "__main__":
    main()
