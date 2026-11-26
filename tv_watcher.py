"""
PSL AI v2.1 - Hafif Doğrudan TradingView WebSocket Motoru (~35 MB RAM)
Tarayıcı (Chrome/Docker) gerektirmeden TradingView'ın veri soketi üzerinden
Kripto Pusulası'nın orijinal PSL AI v2.1 indikatörünü doğrudan çalıştırır ve
tüm plot/tablo sinyallerini Bybit botuna iletir.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import string
import threading
import time
from typing import Any, Callable, Optional

import aiohttp
import requests

logger = logging.getLogger("psl-tv-watcher")

ENGINE_MODE = os.getenv("ENGINE_MODE", "QUANT_5M").strip().upper()
QUANT_MODE = os.getenv("QUANT_MODE", "VELEZ_ULTRA").strip().upper()

PSL_SCRIPT_ID = "PUB;dc403b2eacfd4347a5e1b6244f203220"
PSL_TRANSLATE_URL = f"https://pine-facade.tradingview.com/pine-facade/translate/{PSL_SCRIPT_ID}/last"
TV_WS_URL = "wss://data.tradingview.com/socket.io/websocket?type=chart"
TV_CHART_PAGE_URL = "https://www.tradingview.com/chart/"

PSL_KEYWORDS = (
    "ANALYTICAL INTELLIGENCE",
    "İŞLEM PLANI",
    "TRADE PLAN",
    "GİRİŞ",
    "HEDEF 1",
    "GEÇERSİZ",
    "INVALIDATION",
)


def _normalize_interval(raw: str) -> str:
    val = raw.strip().lower()
    mapping = {
        "1": "1",
        "1m": "1",
        "1d": "1",
        "1dk": "1",
        "3": "3",
        "3m": "3",
        "5": "5",
        "5m": "5",
        "5d": "5",
        "5dk": "5",
        "15": "15",
        "15m": "15",
        "15d": "15",
        "15dk": "15",
        "30": "30",
        "30m": "30",
        "60": "60",
        "1h": "60",
        "1s": "60",
        "120": "120",
        "2h": "120",
        "240": "240",
        "4h": "240",
        "4s": "240",
        "d": "D",
        "1d_day": "D",
        "1g": "D",
    }
    return mapping.get(val, raw.strip().upper() if raw.strip().upper() == "D" else raw.strip())


def _to_tv_symbol(raw: str) -> str:
    """
    Kullanıcı girdisini TradingView Bybit Perpetual formatına çevirir.
    Örn: 'BTCUSDT' -> 'BYBIT:BTCUSDT.P', 'BYBIT:PUMPFUNUSDT.P' -> 'BYBIT:PUMPFUNUSDT.P'
    """
    sym = raw.strip().upper().replace(" ", "")
    if not sym:
        return "BYBIT:BTCUSDT.P"
    if ":" not in sym:
        sym = f"BYBIT:{sym}"
    exchange, ticker = sym.split(":", 1)
    if ticker.endswith(("USDT", "USDC", "USD")) and not ticker.endswith((".P", ".PS", "PERP")):
        ticker = f"{ticker}.P"
    return f"{exchange}:{ticker}"


def _to_bybit_rest_symbol(raw_symbol: str) -> str:
    sym = raw_symbol.strip().upper()
    if ":" in sym:
        sym = sym.split(":")[-1]
    for suffix in (".P", ".PS", "PERP"):
        if sym.endswith(suffix):
            sym = sym[: -len(suffix)]
    sym = sym.replace("/", "")
    return sym


def _parse_price_token(token: str) -> Optional[float]:
    """
    Tablo hücresindeki fiyat metnini sayıya çevirir.
    Yüzde (+0.92%), R:R (1:0.6) ve skor (7/8) ifadelerini otomatik eler.
    """
    clean = token.strip().replace(" ", "")
    if not clean:
        return None
    if any(ch in clean for ch in ("%", ":", "/", "x", "X")):
        return None
    if clean.startswith(("+", "-")):
        return None
    if not re.fullmatch(r"\d+(?:[.,]\d+)*", clean):
        return None

    if "," in clean and "." in clean:
        if clean.rfind(",") < clean.rfind("."):
            clean = clean.replace(",", "")
        else:
            clean = clean.replace(".", "").replace(",", ".")
    elif "," in clean:
        if re.fullmatch(r"\d{1,3}(?:,\d{3})+", clean) and not clean.startswith("0,"):
            clean = clean.replace(",", "")
        else:
            clean = clean.replace(",", ".")

    try:
        val = float(clean)
        return val if val > 0 else None
    except ValueError:
        return None


def _extract_first_price(line: str, label_pattern: str) -> Optional[float]:
    match = re.search(label_pattern, line, flags=re.IGNORECASE)
    if not match:
        return None
    tail = line[match.end() :]
    for part in re.split(r"[\s|]+", tail.strip()):
        price = _parse_price_token(part)
        if price is not None:
            return price
    return None


def _tf_close_pct_list(interval: str) -> list[int]:
    """
    Zaman dilimine göre optimize edilmiş kısmi kâr alma oranlarını döndürür:
    - 1 saat (60m) ve üzeri: [40, 30, 20, 10] (büyük trendleri taşıyıp kârı maksimize eder)
    - 30 dk ve altı: [55, 15, 15, 15] (TP1'de hızlı ağırlık alıp geri dönüş riskini azaltır)
    """
    norm = _normalize_interval(interval or "60").upper()
    if norm in ("60", "120", "240", "D"):
        return [40, 30, 20, 10]
    return [55, 15, 15, 15]


def _tf_sim_leverage(interval: str, fallback: int = 20) -> int:
    """TradingView iç simülasyon tablosunun (in_29) ve Quant motorunun kaldıracını belirler."""
    norm = _normalize_interval(interval or "5").upper()
    mapping = {
        "1": 20,
        "3": 20,
        "5": 20,
        "15": 20,
        "30": 20,
        "60": 15,
        "120": 15,
        "240": 8,
        "D": 8,
    }
    return mapping.get(norm, fallback)


def _interval_to_bybit(interval: str) -> str:
    norm = _normalize_interval(interval or "5").upper()
    return norm if norm in ("1", "3", "5", "15", "30", "60", "120", "240", "D") else "5"


def _interval_ms(interval: str) -> int:
    byb = _interval_to_bybit(interval)
    if byb == "D":
        return 86_400_000
    return int(byb) * 60_000


def _calc_ema(vals: list[float], period: int) -> list[float]:
    n = len(vals)
    out = [0.0] * n
    if n == 0:
        return out
    k = 2.0 / (period + 1.0)
    out[0] = vals[0]
    for i in range(1, n):
        out[i] = vals[i] * k + out[i - 1] * (1.0 - k)
    return out


def _calc_sma(vals: list[float], period: int) -> list[float]:
    n = len(vals)
    out = [0.0] * n
    run = 0.0
    for i in range(n):
        run += vals[i]
        if i >= period:
            run -= vals[i - period]
        out[i] = run / min(i + 1, period)
    return out


def _calc_stdev(vals: list[float], sma_vals: list[float], period: int) -> list[float]:
    n = len(vals)
    out = [0.0] * n
    for i in range(period - 1, n):
        m = sma_vals[i]
        s = 0.0
        for j in range(i - period + 1, i + 1):
            d = vals[j] - m
            s += d * d
        out[i] = math.sqrt(s / period)
    return out


def _calc_atr(bars: list[dict[str, float]], period: int = 14) -> list[float]:
    n = len(bars)
    out = [0.0] * n
    if n == 0:
        return out
    trs = [0.0] * n
    trs[0] = bars[0]["h"] - bars[0]["l"]
    for i in range(1, n):
        h, l, pc = bars[i]["h"], bars[i]["l"], bars[i - 1]["c"]
        trs[i] = max(h - l, abs(h - pc), abs(l - pc))
    run = sum(trs[:period])
    for i in range(n):
        if i < period:
            out[i] = sum(trs[: i + 1]) / (i + 1)
        elif i == period:
            out[i] = (run - trs[0] + trs[i]) / period
        else:
            out[i] = (out[i - 1] * (period - 1) + trs[i]) / period
    return out


def _calc_adx(bars: list[dict[str, float]], period: int = 14) -> list[float]:
    n = len(bars)
    adx = [0.0] * n
    if n <= period * 2:
        return adx
    sm_tr = sm_pdm = sm_mdm = 0.0
    dx_list = [0.0] * n
    for i in range(1, n):
        h, l, ph, pl, pc = bars[i]["h"], bars[i]["l"], bars[i - 1]["h"], bars[i - 1]["l"], bars[i - 1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        up = h - ph
        dn = pl - l
        pdm = up if (up > dn and up > 0) else 0.0
        mdm = dn if (dn > up and dn > 0) else 0.0
        if i <= period:
            sm_tr += tr
            sm_pdm += pdm
            sm_mdm += mdm
        else:
            sm_tr = sm_tr - (sm_tr / period) + tr
            sm_pdm = sm_pdm - (sm_pdm / period) + pdm
            sm_mdm = sm_mdm - (sm_mdm / period) + mdm
        if i >= period and sm_tr > 0:
            pdi = 100.0 * sm_pdm / sm_tr
            mdi = 100.0 * sm_mdm / sm_tr
            s = pdi + mdi
            dx_list[i] = 100.0 * abs(pdi - mdi) / s if s > 0 else 0.0
    start_adx = period * 2
    if start_adx < n:
        adx[start_adx] = sum(dx_list[period + 1 : start_adx + 1]) / period
        for i in range(start_adx + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx_list[i]) / period
    return adx


def _calc_supertrend(
    bars: list[dict[str, float]], atr_vals: list[float], mult: float = 6.0
) -> tuple[list[int], list[float]]:
    n = len(bars)
    dirs = [1] * n
    line = [0.0] * n
    f_ub = [0.0] * n
    f_lb = [0.0] * n
    for i in range(n):
        hl2 = (bars[i]["h"] + bars[i]["l"]) * 0.5
        ub = hl2 + mult * atr_vals[i]
        lb = hl2 - mult * atr_vals[i]
        if i == 0:
            f_ub[0], f_lb[0], dirs[0], line[0] = ub, lb, 1, lb
            continue
        pc = bars[i - 1]["c"]
        f_ub[i] = ub if (ub < f_ub[i - 1] or pc > f_ub[i - 1]) else f_ub[i - 1]
        f_lb[i] = lb if (lb > f_lb[i - 1] or pc < f_lb[i - 1]) else f_lb[i - 1]
        c = bars[i]["c"]
        if dirs[i - 1] == 1:
            if c < f_lb[i]:
                dirs[i], line[i] = -1, f_ub[i]
            else:
                dirs[i], line[i] = 1, f_lb[i]
        else:
            if c > f_ub[i]:
                dirs[i], line[i] = 1, f_lb[i]
            else:
                dirs[i], line[i] = -1, f_ub[i]
    return dirs, line


def _calc_velez_pct(
    bars: list[dict[str, float]],
    sma20: list[float],
    sma200: list[float],
    atr14: list[float],
    color_len: int = 15,
    slope_len: int = 3,
    slope_min: float = 0.12,
) -> tuple[int, int, dict[str, Any]]:
    """Velez PCT (Position - Dominant Color - 20 SMA Trend) skorunu (0-3) hesaplar."""
    n = len(bars)
    meta = {
        "pos_plus": False,
        "pos_minus": False,
        "dom_green": False,
        "dom_red": False,
        "trend_up": False,
        "trend_down": False,
    }
    if n < max(205, color_len + slope_len + 2):
        return 0, 0, meta

    c = bars[-1]["c"]
    s20 = sma20[-1]
    s200 = sma200[-1]
    atr = max(atr14[-1], 1e-12)

    pos_plus = (c > s20) and (c > s200)
    pos_minus = (c < s20) and (c < s200)

    g_sum = r_sum = 0.0
    g_cnt = r_cnt = 0
    cg = cr = mg = mr = 0
    for b in bars[n - color_len : n]:
        bo, bc = b["o"], b["c"]
        bb = abs(bc - bo)
        if bc > bo:
            g_sum += bb
            g_cnt += 1
            cg += 1
            cr = 0
            if cg > mg:
                mg = cg
        elif bc < bo:
            r_sum += bb
            r_cnt += 1
            cr += 1
            cg = 0
            if cr > mr:
                mr = cr
        else:
            cg = 0
            cr = 0

    g_score = int(g_sum > r_sum) + int(mg > mr) + int(g_cnt > r_cnt)
    r_score = int(r_sum > g_sum) + int(mr > mg) + int(r_cnt > g_cnt)
    dom_green = g_score >= 2
    dom_red = r_score >= 2

    s20_prev = sma20[-1 - slope_len]
    trend_up = (s20 - s20_prev) > (atr * slope_min)
    trend_down = (s20_prev - s20) > (atr * slope_min)

    meta = {
        "pos_plus": pos_plus,
        "pos_minus": pos_minus,
        "dom_green": dom_green,
        "dom_red": dom_red,
        "trend_up": trend_up,
        "trend_down": trend_down,
    }
    pct_long = int(pos_plus) + int(dom_green) + int(trend_up)
    pct_short = int(pos_minus) + int(dom_red) + int(trend_down)
    return pct_long, pct_short, meta


def parse_psl_table(lines: list[str]) -> dict[str, Any]:
    """TradingView tablo hücrelerinden PSL AI v2.1 panel verilerini ayıklar."""
    result: dict[str, Any] = {
        "indicator_detected": False,
        "verdict": "",
        "plan_side": "",
        "plan_status": "unknown",
        "plan_status_text": "",
        "last_sim_trade": "",
        "checked_tps": [],
        "entry": None,
        "tp1": None,
        "tp2": None,
        "tp3": None,
        "tp4": None,
        "sl": None,
    }
    if not lines:
        return result

    upper_lines = [line.upper() for line in lines]
    joined_upper = "\n".join(upper_lines)

    if any(k in joined_upper for k in PSL_KEYWORDS):
        result["indicator_detected"] = True

    for idx, uline in enumerate(upper_lines):
        if "SONUÇ" in uline or "RESULT" in uline:
            for verdict in (
                "ZAYIF AL",
                "ZAYIF SAT",
                "HAFİF AL",
                "HAFİF SAT",
                "WEAK BUY",
                "WEAK SELL",
                "NÖTR",
                "NEUTRAL",
                "AL",
                "SAT",
                "BUY",
                "SELL",
            ):
                if re.search(rf"(?<![A-ZÇĞİÖŞÜ]){re.escape(verdict)}(?![A-ZÇĞİÖŞÜ])", uline):
                    result["verdict"] = verdict
                    break

        if "İŞLEM PLANI" in uline or "TRADE PLAN" in uline:
            if any(w in uline for w in ("ALIŞ", "LONG", "BUY")):
                result["plan_side"] = "long"
            elif any(w in uline for w in ("SATIŞ", "SHORT", "SELL")):
                result["plan_side"] = "short"

            header_window = uline
            if idx + 1 < len(upper_lines) and "SEVİYE" not in upper_lines[idx + 1] and "LEVEL" not in upper_lines[idx + 1]:
                header_window = f"{uline} | {upper_lines[idx + 1]}"

            if any(w in header_window for w in ("ÜST TF UYUMSUZ", "UYUMSUZ", "MISMATCH")):
                result["plan_status"] = "mismatch"
                result["plan_status_text"] = "ÜST TF UYUMSUZ"
            elif any(w in header_window for w in ("TETİK BEKLENİYOR", "TETİK BEKLİYOR", "AWAITING TRIGGER", "BEKLENİYOR", "BEKLİYOR", "WAITING")):
                result["plan_status"] = "waiting"
                result["plan_status_text"] = "TETİK BEKLENİYOR"
            elif any(w in header_window for w in ("AKTİF", "ACTIVE")):
                result["plan_status"] = "active"
                result["plan_status_text"] = "🟢 AKTİF (İŞLEM AÇIK)"
            elif any(w in header_window for w in ("HEDEFLERE ULAŞILDI", "TARGETS REACHED", "TAMAMLANDI")):
                result["plan_status"] = "completed"
                result["plan_status_text"] = "HEDEFLERE ULAŞILDI"
            elif any(w in header_window for w in ("GEÇERSİZ OLDU", "STOPPED", "KAPANDI", "CLOSED")):
                result["plan_status"] = "stopped"
                result["plan_status_text"] = "KAPANDI"
            elif "—" in header_window or "| -" in header_window:
                result["plan_status"] = "none"
                result["plan_status_text"] = "Plan Yok (—)"

        if not result["last_sim_trade"]:
            sim_match = re.match(
                r"^(\d+)\s*\|\s*(ALIŞ|SATIŞ|LONG|SHORT)\s*\|\s*[^|]+\|\s*([^|]+)\|\s*([^|]+)",
                uline,
            )
            if sim_match:
                t_num, t_side, t_res, t_pnl = sim_match.groups()
                result["last_sim_trade"] = f"#{t_num.strip()} {t_side.strip()} · {t_res.strip()} ({t_pnl.strip()})"

        # Yorum (▍) ve simülasyon geçmişi satırlarından sahte GİRİŞ rakamı okuma
        if uline.startswith("▍") or uline.startswith("# |") or "SONUÇLANAN" in uline:
            continue

        if "GİRİŞ" in uline or "ENTRY" in uline:
            if any(w in uline for w in ("İŞLEM AÇIK", "TRADE OPEN")):
                result["plan_status"] = "active"
                result["plan_status_text"] = "🟢 AKTİF (İŞLEM AÇIK)"
            if result["entry"] is None:
                val = _extract_first_price(uline, r"\b(?:GİRİŞ|ENTRY)\b")
                if val is not None:
                    result["entry"] = val

        for tp_idx in range(1, 5):
            tp_key = f"tp{tp_idx}"
            tp_pat = rf"\b(?:HEDEF\s*{tp_idx}|TARGET\s*{tp_idx}|TP\s*{tp_idx})\b"
            if re.search(tp_pat, uline):
                if ("✓" in uline or "✔" in uline) and tp_key not in result["checked_tps"]:
                    result["checked_tps"].append(tp_key)
                if result[tp_key] is None:
                    val = _extract_first_price(uline, tp_pat)
                    if val is not None:
                        result[tp_key] = val

        if result["sl"] is None:
            val = _extract_first_price(uline, r"\b(?:GEÇERSİZ|INVALIDATION|STOP)\b")
            if val is not None:
                result["sl"] = val

    if result["plan_status"] != "active" and result["last_sim_trade"]:
        if result["plan_status_text"]:
            result["plan_status_text"] = f"{result['plan_status_text']} · Son: {result['last_sim_trade']}"
        else:
            result["plan_status_text"] = result["last_sim_trade"]
    result["raw_lines"] = lines[:45]
    return result


# ---------------------------------------------------------------------------
# TradingView Socket.IO Paketleme ve Grafik Çözümleyicileri
# ---------------------------------------------------------------------------
def _gen_session(prefix: str) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase, k=12))


def _pack_tv(method: str, params: list[Any]) -> str:
    body = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(body)}~m~{body}"


def _unpack_tv_frames(raw_msg: str) -> list[str]:
    frames: list[str] = []
    idx = 0
    total = len(raw_msg)
    while idx < total:
        if not raw_msg.startswith("~m~", idx):
            break
        len_start = idx + 3
        len_end = raw_msg.find("~m~", len_start)
        if len_end == -1:
            break
        try:
            frame_len = int(raw_msg[len_start:len_end])
        except ValueError:
            break
        payload_start = len_end + 3
        payload_end = payload_start + frame_len
        frames.append(raw_msg[payload_start:payload_end])
        idx = payload_end
    return frames


def _clean_num(val: Any) -> Optional[float]:
    if not isinstance(val, (int, float)):
        return None
    fval = float(val)
    if math.isnan(fval) or math.isinf(fval) or abs(fval) >= 1e50:
        return None
    return fval


def _is_triggered(val: Any) -> bool:
    num = _clean_num(val)
    return num is not None and num >= 0.5


def _extract_table_lines_from_ns(ns_d: Any) -> list[str]:
    """TradingView `st1.ns.d` JSON verisindeki dwgtablecells metinlerini satırlara döker."""
    if not isinstance(ns_d, str) or ("tableCells" not in ns_d and "dwgtablecells" not in ns_d and "GİRİŞ" not in ns_d):
        return []
    try:
        parsed = json.loads(ns_d)
    except Exception:
        return []

    rows_map: dict[tuple[int, int], list[tuple[int, str]]] = {}

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            row_val = node.get("row", node.get("r"))
            col_val = node.get("col", node.get("c"))
            if row_val is not None and col_val is not None and isinstance(node.get("t"), str):
                txt = node["t"].replace("\n", " ").strip()
                if txt:
                    tid = int(node.get("tid", 1))
                    r_idx = int(row_val)
                    c_idx = int(col_val)
                    rows_map.setdefault((tid, r_idx), []).append((c_idx, txt))
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(parsed)
    lines: list[str] = []
    for key in sorted(rows_map.keys()):
        cols = [t for _, t in sorted(rows_map[key], key=lambda item: item[0])]
        if cols:
            lines.append(" | ".join(cols))
    return lines


class TradingViewPlaywrightWatcher:
    """
    Hafif TradingView WebSocket izleyicisi (bot.py ile tam uyumlu sınıf adı).
    Bir veya birden fazla coini doğrudan TradingView sunucusunda çalıştırılan
    PSL AI v2.1 (PUB;dc403b2eacfd4347a5e1b6244f203220) üzerinden anlık izler.
    """

    def __init__(
        self,
        tv_symbol: str,
        tv_interval: str,
        signal_callback: Callable[[dict[str, Any], str], Any],
        chart_url: str = "",
        sessionid: str = "",
        sessionid_sign: str = "",
        poll_sec: float = 2.0,
        secret_key: str = "",
        default_leverage: int = 50,
        bybit_testnet: bool = False,
    ) -> None:
        raw_list = [s.strip() for s in tv_symbol.split(",") if s.strip()]
        seen_syms: list[str] = []
        for item in raw_list:
            norm = _to_tv_symbol(item)
            if norm not in seen_syms:
                seen_syms.append(norm)
        self.tv_symbols: list[str] = seen_syms[:15] or ["BYBIT:BTCUSDT.P"]
        self.tv_symbol = self.tv_symbols[0]
        self.tv_interval = _normalize_interval(tv_interval or "5")
        self.engine_mode = os.getenv("ENGINE_MODE", ENGINE_MODE).strip().upper()
        self.quant_mode = os.getenv("QUANT_MODE", QUANT_MODE).strip().upper()
        self.chart_url = chart_url.strip()
        self.sessionid = sessionid.strip()
        self.sessionid_sign = sessionid_sign.strip()
        self.poll_sec = max(1.0, poll_sec)
        self.secret_key = secret_key
        self.default_leverage = default_leverage
        self.bybit_api_base = "https://api-testnet.bybit.com" if bybit_testnet else "https://api.bybit.com"
        self._signal_callback = signal_callback

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._state_lock = threading.Lock()
        self._trade_seq = int(time.time()) % 100000
        self._http = requests.Session()

        self._coin_states: dict[str, dict[str, Any]] = {}
        for sym in self.tv_symbols:
            self._coin_states[sym] = {
                "symbol": sym,
                "interval": self.tv_interval,
                "bybit_rest_symbol": _to_bybit_rest_symbol(sym),
                "indicator_detected": False,
                "live_price": None,
                "parsed_table": {},
                "active_trade": None,
                "last_entered_sig": None,
                "last_closed_ts": None,
                "non_active_streak": 0,
                "last_update": None,
                "needs_login_hint": False,
                "last_error": None,
            }

        self.latest_status: dict[str, Any] = {
            "running": False,
            "state": "Başlatılıyor...",
            "chart_url": f"https://tr.tradingview.com/chart/?symbol={self.tv_symbol}&interval={self.tv_interval}",
            "symbol": self.tv_symbol,
            "symbols": self.tv_symbols,
            "interval": self.tv_interval,
            "indicator_detected": False,
            "live_price": None,
            "parsed_table": {},
            "active_trade": None,
            "coins": {},
            "last_update": None,
            "last_error": None,
            "needs_login_hint": False,
        }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="tv-ws-watcher",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def get_status(self) -> dict[str, Any]:
        with self._state_lock:
            return dict(self.latest_status)

    def get_screenshot(self) -> Optional[bytes]:
        # Doğrudan WebSocket modunda tarayıcı/ekran görüntüsü gerekmez
        return None

    def _update_status(self, **kwargs: Any) -> None:
        with self._state_lock:
            self.latest_status.update(kwargs)

    def _sync_coins_status(self, state_label: Optional[str] = None) -> None:
        with self._state_lock:
            snapshot = {
                sym: {
                    "symbol": st["symbol"],
                    "interval": st["interval"],
                    "indicator_detected": st["indicator_detected"],
                    "live_price": st["live_price"],
                    "parsed_table": st["parsed_table"],
                    "active_trade": st["active_trade"],
                    "last_update": st["last_update"],
                    "needs_login_hint": st["needs_login_hint"],
                }
                for sym, st in self._coin_states.items()
            }
            primary = snapshot[self.tv_symbol]
            if state_label is not None:
                self.latest_status["state"] = state_label
            self.latest_status.update(
                {
                    "indicator_detected": primary["indicator_detected"],
                    "live_price": primary["live_price"],
                    "parsed_table": primary["parsed_table"],
                    "active_trade": primary["active_trade"],
                    "coins": snapshot,
                    "last_update": primary["last_update"],
                    "needs_login_hint": any(c["needs_login_hint"] for c in snapshot.values()),
                }
            )

    def _fetch_tv_auth_token(self) -> Optional[str]:
        """TradingView sessionid çerezinden WebSocket auth_token alır."""
        if not self.sessionid:
            return None

        cookie_parts = [f"sessionid={self.sessionid}"]
        if self.sessionid_sign:
            cookie_parts.append(f"sessionid_sign={self.sessionid_sign}")

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
            ),
            "Cookie": "; ".join(cookie_parts),
        }
        try:
            resp = requests.get(TV_CHART_PAGE_URL, headers=headers, timeout=15)
            match = re.search(r'"auth_token"\s*:\s*"([^"]+)"', resp.text)
            if match:
                token = match.group(1)
                if token != "unauthorized_user_token":
                    return token
        except Exception as exc:
            logger.error("TradingView auth_token alınırken hata: %s", exc)
        return None

    def _fetch_psl_study_args(self) -> dict[str, Any]:
        """TradingView pine-facade üzerinden güncel PSL AI v2.1 meta verilerini indirir."""
        resp = requests.get(
            PSL_TRANSLATE_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        meta = (data.get("result") or {}).get("metaInfo") or {}
        inputs = (meta.get("defaults") or {}).get("inputs") or {}
        sim_lev = _tf_sim_leverage(self.tv_interval, self.default_leverage)
        return {
            "text": inputs["text"],
            "pineId": meta["scriptIdPart"],
            "pineVersion": (meta.get("pine") or {}).get("version", "3.0"),
            "in_27": {"v": 100, "f": True, "t": "integer"},
            "in_28": {"v": 15, "f": True, "t": "integer"},
            "in_29": {"v": sim_lev, "f": True, "t": "integer"},
            "in_39": {"v": True, "f": True, "t": "bool"},
        }

    def _run_loop(self) -> None:
        if self.engine_mode != "PSL_TV":
            self._run_quant_loop()
            return

        logger.info("TradingView WebSocket izleyicisi başlatılıyor: %s", ", ".join(self.tv_symbols))
        self._update_status(running=True, state="TradingView bağlantısı hazırlanıyor...")

        while not self._stop_event.is_set():
            auth_token = self._fetch_tv_auth_token()
            if not auth_token:
                msg = (
                    "TV_SESSIONID çerezi eksik veya süresi dolmuş. "
                    "Render Environment kısmına geçerli TV_SESSIONID ekleyin."
                )
                logger.warning(msg)
                for st in self._coin_states.values():
                    st["needs_login_hint"] = True
                self._sync_coins_status(state_label="TV_SESSIONID bekleniyor")
                self._update_status(last_error=msg)
                self._stop_event.wait(15)
                continue

            try:
                study_args = self._fetch_psl_study_args()
            except Exception as exc:
                logger.error("PSL AI indikatör verisi indirilemedi: %s", exc)
                self._update_status(state="İndikatör indirilemedi, tekrar deneniyor...", last_error=str(exc))
                self._stop_event.wait(10)
                continue

            try:
                asyncio.run(self._run_all_symbols(auth_token, study_args))
            except Exception as exc:
                logger.error("WebSocket döngüsü hatası: %s", exc)
                self._update_status(state="Yeniden bağlanıyor...", last_error=str(exc))
                self._stop_event.wait(5)

        self._update_status(running=False, state="Durduruldu")

    async def _run_all_symbols(self, auth_token: str, study_args: dict[str, Any]) -> None:
        self._sync_coins_status(state_label=f"Canlı WebSocket izleniyor ({len(self.tv_symbols)} parite)")
        tasks = [
            asyncio.create_task(self._watch_symbol_ws(sym, auth_token, study_args, delay_sec=idx * 1.2))
            for idx, sym in enumerate(self.tv_symbols)
        ]
        while not self._stop_event.is_set():
            if all(t.done() for t in tasks):
                break
            await asyncio.sleep(1.0)
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _watch_symbol_ws(
        self,
        sym: str,
        auth_token: str,
        study_args: dict[str, Any],
        delay_sec: float = 0.0,
    ) -> None:
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)
        state = self._coin_states[sym]
        headers = {
            "Origin": "https://www.tradingview.com",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
            ),
        }

        while not self._stop_event.is_set():
            chart_session = _gen_session("cs_")
            study_ready = False
            latest_bar_time: Optional[int] = None
            latest_bar_plots: Optional[list[Any]] = None
            sds_latest_bar_time: Optional[int] = None
            sds_latest_close: Optional[float] = None
            fired_keys: set[str] = set()
            last_entry_bar_time: Optional[int] = None

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(TV_WS_URL, headers=headers, heartbeat=20) as ws:
                        await ws.send_str(_pack_tv("set_auth_token", [auth_token]))
                        await ws.send_str(_pack_tv("chart_create_session", [chart_session, ""]))
                        sym_cfg = "=" + json.dumps({"symbol": sym, "adjustment": "splits"})
                        await ws.send_str(_pack_tv("resolve_symbol", [chart_session, "sds_sym_1", sym_cfg]))
                        await ws.send_str(_pack_tv("create_series", [chart_session, "sds_1", "s1", "sds_sym_1", self.tv_interval, 3000, ""]))
                        await ws.send_str(_pack_tv("create_study", [chart_session, "st1", "st1", "sds_1", "Script@tv-scripting-101!", study_args]))
                        logger.info("TradingView WebSocket başlatıldı: %s (%s)", sym, self.tv_interval)

                        async for msg in ws:
                            if self._stop_event.is_set():
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue

                            for frame in _unpack_tv_frames(msg.data):
                                if frame.startswith("~h~"):
                                    await ws.send_str(f"~m~{len(frame)}~m~{frame}")
                                    continue

                                try:
                                    pkt = json.loads(frame)
                                except json.JSONDecodeError:
                                    continue

                                m_type = pkt.get("m")
                                params = pkt.get("p") or []

                                if m_type == "study_error":
                                    err_obj = params[3] if len(params) > 3 else params
                                    logger.error("TradingView study_error (%s): %s", sym, err_obj)
                                    err_str = str(err_obj)
                                    if "study_limit" in err_str or "unauthorized" in err_str.lower():
                                        state["needs_login_hint"] = True
                                        self._sync_coins_status(state_label="TV_SESSIONID yetkisiz veya limit dolu")
                                        return
                                    continue

                                if m_type == "study_completed":
                                    study_ready = True
                                    state["indicator_detected"] = True
                                    state["needs_login_hint"] = False
                                    state["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")
                                    self._evaluate_live_table_and_price(
                                        sym,
                                        state,
                                        state.get("parsed_table") or {},
                                        None,
                                        latest_bar_plots,
                                        sds_latest_bar_time,
                                        table_updated=True,
                                    )
                                    self._sync_coins_status()
                                    continue

                                if m_type != "du" or len(params) < 2 or not isinstance(params[1], dict):
                                    continue

                                payload_dict = params[1]
                                sds_closed_bar: Optional[tuple[int, float]] = None

                                # Anlık mum fiyatını ve kapanan mumu güncelle (sds_1)
                                sds_data = payload_dict.get("sds_1")
                                if isinstance(sds_data, dict):
                                    for sbar in sds_data.get("s") or []:
                                        bv = sbar.get("v") or []
                                        if len(bv) < 5:
                                            continue
                                        bt_raw = _clean_num(bv[0])
                                        bc = _clean_num(bv[4])
                                        if bt_raw is None or bc is None:
                                            continue
                                        bt = int(bt_raw)
                                        if (
                                            study_ready
                                            and sds_latest_bar_time is not None
                                            and bt > sds_latest_bar_time
                                            and sds_latest_close is not None
                                        ):
                                            sds_closed_bar = (sds_latest_bar_time, sds_latest_close)
                                        sds_latest_bar_time = bt
                                        sds_latest_close = bc
                                        state["live_price"] = bc

                                # PSL AI study verisini işle (st1)
                                table_updated = False
                                st_data = payload_dict.get("st1")
                                if isinstance(st_data, dict):
                                    state["indicator_detected"] = True
                                    state["needs_login_hint"] = False
                                    state["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")

                                    ns_d = (st_data.get("ns") or {}).get("d")
                                    table_lines = _extract_table_lines_from_ns(ns_d)
                                    table_updated = bool(table_lines)
                                    prev_parsed = state.get("parsed_table") or {}
                                    parsed_table = dict(prev_parsed)
                                    if table_lines:
                                        parsed_table = parse_psl_table(table_lines)
                                        if not parsed_table.get("last_sim_trade") and prev_parsed.get("last_sim_trade"):
                                            parsed_table["last_sim_trade"] = prev_parsed["last_sim_trade"]
                                            if parsed_table.get("plan_status") != "active":
                                                base_txt = parsed_table.get("plan_status_text") or ""
                                                parsed_table["plan_status_text"] = (
                                                    f"{base_txt} · Son: {prev_parsed['last_sim_trade']}"
                                                    if base_txt
                                                    else prev_parsed["last_sim_trade"]
                                                )

                                    bars = st_data.get("st") or st_data.get("s") or []
                                    for bar in bars:
                                        v = bar.get("v") or []
                                        if len(v) < 7:
                                            continue
                                        bar_t_raw = _clean_num(v[0])
                                        if bar_t_raw is None:
                                            continue
                                        bar_t = int(bar_t_raw)

                                        if (
                                            study_ready
                                            and latest_bar_time is not None
                                            and bar_t > latest_bar_time
                                            and latest_bar_plots is not None
                                        ):
                                            self._process_closed_bar(
                                                sym,
                                                state,
                                                latest_bar_time,
                                                latest_bar_plots,
                                                fired_keys,
                                            )
                                            if (
                                                _is_triggered(self._plot(latest_bar_plots, 9))
                                                or _is_triggered(self._plot(latest_bar_plots, 10))
                                            ):
                                                last_entry_bar_time = latest_bar_time

                                        latest_bar_time = bar_t
                                        latest_bar_plots = v

                                        if study_ready:
                                            self._process_intrabar_events(
                                                sym,
                                                state,
                                                bar_t,
                                                v,
                                                fired_keys,
                                                last_entry_bar_time,
                                            )

                                    if latest_bar_plots is not None:
                                        self._sync_levels_from_plots(parsed_table, latest_bar_plots)

                                    state["parsed_table"] = parsed_table

                                if study_ready and (isinstance(st_data, dict) or isinstance(sds_data, dict)):
                                    self._evaluate_live_table_and_price(
                                        sym,
                                        state,
                                        state.get("parsed_table") or {},
                                        sds_closed_bar,
                                        latest_bar_plots,
                                        sds_latest_bar_time,
                                        table_updated=table_updated,
                                    )
                                    self._sync_coins_status()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("TradingView WebSocket bağlantısı koptu (%s), 5 sn sonra yeniden bağlanılacak: %s", sym, exc)
                await asyncio.sleep(5)

    @staticmethod
    def _plot(v: list[Any], idx: int) -> Any:
        return v[idx] if idx < len(v) else None

    def _sync_levels_from_plots(self, parsed: dict[str, Any], v: list[Any]) -> None:
        """
        v[1]=Entry, v[2]=Stop, v[3]=TP1, v[4]=TP2, v[5]=TP3, v[6]=TP4
        Tablo aktif veya tetik bekliyorsa tablo değerlerini korur; yalnızca tablo boşsa ('—')
        son işlem plotlarını yedek olarak kullanır.
        """
        if parsed.get("plan_status") in ("active", "waiting", "mismatch") and parsed.get("entry") is not None:
            return
        if parsed.get("entry") is not None:
            return

        mapping = [(1, "entry"), (2, "sl"), (3, "tp1"), (4, "tp2"), (5, "tp3"), (6, "tp4")]
        any_level = False
        for idx, field in mapping:
            num = _clean_num(self._plot(v, idx))
            if num is not None and num > 0:
                parsed[field] = num
                any_level = True
        if parsed.get("entry") and parsed.get("tp1"):
            parsed["plan_side"] = "long" if parsed["tp1"] > parsed["entry"] else "short"
        if any_level:
            parsed["indicator_detected"] = True

    def _evaluate_live_table_and_price(
        self,
        sym: str,
        state: dict[str, Any],
        parsed_table: dict[str, Any],
        sds_closed_bar: Optional[tuple[int, float]],
        latest_plots: Optional[list[Any]],
        sds_bar_time: Optional[int],
        table_updated: bool = False,
    ) -> None:
        """
        study_completed sonrası TradingView st1.st boş geldiğinde tüm canlı işlemleri
        st1.ns.d tablosu ve sds_1 canlı fiyat/mum kapanışı üzerinden yürütür.
        """
        plan_status = parsed_table.get("plan_status")
        plan_side = parsed_table.get("plan_side")
        entry = parsed_table.get("entry")
        tp1 = parsed_table.get("tp1")
        tp2 = parsed_table.get("tp2")
        tp3 = parsed_table.get("tp3")
        tp4 = parsed_table.get("tp4")
        sl = parsed_table.get("sl")
        checked_tps = parsed_table.get("checked_tps") or []
        live_price = state.get("live_price")
        bar_t = sds_bar_time or int(time.time())
        plots = latest_plots or []

        has_levels = all(v is not None and v > 0 for v in (entry, tp1, tp2, tp3, tp4, sl))

        # 1. Açık İşlem Takibi
        active = state.get("active_trade")
        if active is not None:
            a_side = active["side"]
            # Orta mumda ÜST TF UYUMSUZ / stopped yanıp sönmelerinde erken stop yapma;
            # yalnızca mum kapanışında ters yöne aktif plan kesinleşirse eski pozisyonu kapat.
            if (
                sds_closed_bar is not None
                and plan_status == "active"
                and plan_side in ("long", "short")
                and plan_side != a_side
                and has_levels
            ):
                self._emit_event(sym, "stop", a_side, sds_closed_bar[1] or live_price or active["sl"], active, sds_closed_bar[0], plots)
                state["active_trade"] = None
                state["non_active_streak"] = 0
                active = None

        active = state.get("active_trade")
        if active is not None:
            a_side = active["side"]
            tf_closes = _tf_close_pct_list(self.tv_interval)

            if not active["hit_tps"] and plan_status == "active" and sl is not None and sl > 0:
                active["sl"] = sl

            # TP1 - TP4 anlık kontrol (✓ işareti veya fiyat teması)
            tp_just_hit = False
            for idx in range(1, 5):
                tp_key = f"tp{idx}"
                if tp_key in active["hit_tps"]:
                    continue
                tp_val = active[tp_key]
                touched = tp_key in checked_tps
                if not touched and live_price is not None and tp_val:
                    touched = (
                        (a_side == "long" and live_price >= tp_val)
                        or (a_side == "short" and live_price <= tp_val)
                    )
                if touched:
                    active["hit_tps"].append(tp_key)
                    tp_just_hit = True
                    if idx == 1:
                        active["sl"] = active["entry"]
                    elif idx == 2:
                        active["sl"] = active["tp1"]
                    elif idx == 3:
                        active["sl"] = active["tp2"]

                    pct_share = tf_closes[idx - 1]
                    self._emit_event(
                        sym,
                        tp_key,
                        a_side,
                        live_price or tp_val,
                        active,
                        bar_t,
                        plots,
                        close_pct=pct_share,
                        new_sl=active["sl"],
                    )
                    if idx == 4:
                        state["active_trade"] = None
                        return

            # TP1+ alındıktan sonra iz süren stopu canlı fiyatta anlık koru (TP vurulduğu aynı tick hariç)
            if not tp_just_hit and active["hit_tps"] and live_price is not None and active.get("sl"):
                trail_sl = active["sl"]
                trail_hit = (
                    (a_side == "long" and live_price <= trail_sl)
                    or (a_side == "short" and live_price >= trail_sl)
                )
                if trail_hit:
                    self._emit_event(sym, "stop", a_side, live_price, active, bar_t, plots)
                    state["active_trade"] = None
                    return

            # Mum kapanışı Stop kontrolü (sds_1 mum kapanışı)
            if sds_closed_bar is not None:
                closed_t, closed_p = sds_closed_bar
                if closed_t * 1000 >= active["opened_candle_ms"]:
                    stopped = (
                        (a_side == "long" and closed_p <= active["sl"])
                        or (a_side == "short" and closed_p >= active["sl"])
                    )
                    if stopped:
                        self._emit_event(sym, "stop", a_side, closed_p, active, closed_t, plots)
                        state["active_trade"] = None
                        return

        # 2. Yeni Giriş Kontrolü
        if state.get("active_trade") is None and plan_status == "active" and plan_side in ("long", "short") and has_levels:
            plan_sig = f"{sym}|{plan_side}|{entry}|{tp4}"
            if plan_sig == state.get("last_entered_sig"):
                return

            if checked_tps:
                # Bot açılmadan önce zaten TP almış devam eden eski pozisyona sonradan girme
                state["last_entered_sig"] = plan_sig
                logger.info("%s devam eden işlem (zaten TP almış: %s), yeni giriş açılmadı.", sym, checked_tps)
                return

            self._trade_seq += 1
            plan = {
                "trade_id": self._trade_seq,
                "side": plan_side,
                "entry": entry,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "tp4": tp4,
                "sl": sl,
                "hit_tps": [],
                "opened_candle_ms": bar_t * 1000,
            }
            state["active_trade"] = plan
            state["last_entered_sig"] = plan_sig
            state["non_active_streak"] = 0
            self._emit_event(sym, "entry", plan_side, live_price or entry, plan, bar_t, plots)

    def _process_closed_bar(
        self,
        sym: str,
        state: dict[str, Any],
        bar_t: int,
        v: list[Any],
        fired_keys: set[str],
    ) -> None:
        long_hit = _is_triggered(self._plot(v, 9))
        short_hit = _is_triggered(self._plot(v, 10))
        stop_hit = _is_triggered(self._plot(v, 16))
        liq_hit = _is_triggered(self._plot(v, 17))
        closed_hit = _is_triggered(self._plot(v, 18))

        if long_hit or short_hit:
            side = "long" if long_hit else "short"
            fkey = f"entry|{bar_t}|{side}"
            if fkey not in fired_keys:
                fired_keys.add(fkey)
                self._start_and_emit_entry(sym, state, side, bar_t, v)

        if stop_hit or (closed_hit and not _is_triggered(self._plot(v, 15)) and not liq_hit):
            fkey = f"stop|{bar_t}"
            active = state.get("active_trade")
            if fkey not in fired_keys and active:
                fired_keys.add(fkey)
                exit_p = _clean_num(self._plot(v, 7)) or active.get("sl") or state.get("live_price") or 0.0
                self._emit_event(sym, "stop", active["side"], exit_p, active, bar_t, v)
                state["active_trade"] = None

    def _process_intrabar_events(
        self,
        sym: str,
        state: dict[str, Any],
        bar_t: int,
        v: list[Any],
        fired_keys: set[str],
        last_entry_bar_time: Optional[int],
    ) -> None:
        # Eğer mum kapanışı zaten confirmed entry üretiyorsa anlık giriş yalnızca yeni plan yoksa bekle
        active = state.get("active_trade")
        if not active:
            return

        a_side = active["side"]
        tf_closes = _tf_close_pct_list(self.tv_interval)
        for tp_idx, plot_idx in ((1, 12), (2, 13), (3, 14), (4, 15)):
            if _is_triggered(self._plot(v, plot_idx)):
                tp_key = f"tp{tp_idx}"
                fkey = f"{tp_key}|{active['trade_id']}"
                if fkey not in fired_keys:
                    fired_keys.add(fkey)
                    if tp_key not in active["hit_tps"]:
                        active["hit_tps"].append(tp_key)
                    tp_price = _clean_num(self._plot(v, tp_idx + 2)) or state.get("live_price") or 0.0
                    new_sl = _clean_num(self._plot(v, 2))
                    pct_share = tf_closes[tp_idx - 1]
                    self._emit_event(sym, tp_key, a_side, tp_price, active, bar_t, v, close_pct=pct_share, new_sl=new_sl)
                    if tp_idx == 4:
                        state["active_trade"] = None
                        return

        if _is_triggered(self._plot(v, 17)):
            fkey = f"liq|{active['trade_id']}"
            if fkey not in fired_keys:
                fired_keys.add(fkey)
                exit_p = _clean_num(self._plot(v, 7)) or state.get("live_price") or 0.0
                self._emit_event(sym, "liquidation", a_side, exit_p, active, bar_t, v)
                state["active_trade"] = None

    def _start_and_emit_entry(
        self,
        sym: str,
        state: dict[str, Any],
        side: str,
        bar_t: int,
        v: list[Any],
    ) -> None:
        entry_p = _clean_num(self._plot(v, 1)) or state.get("live_price") or 0.0
        sl_p = _clean_num(self._plot(v, 2)) or 0.0
        tp1_p = _clean_num(self._plot(v, 3)) or 0.0
        tp2_p = _clean_num(self._plot(v, 4)) or 0.0
        tp3_p = _clean_num(self._plot(v, 5)) or 0.0
        tp4_p = _clean_num(self._plot(v, 6)) or 0.0

        self._trade_seq += 1
        plan = {
            "trade_id": self._trade_seq,
            "side": side,
            "entry": entry_p,
            "tp1": tp1_p,
            "tp2": tp2_p,
            "tp3": tp3_p,
            "tp4": tp4_p,
            "sl": sl_p,
            "hit_tps": [],
            "opened_candle_ms": bar_t * 1000,
        }
        state["active_trade"] = plan
        self._emit_event(sym, "entry", side, entry_p, plan, bar_t, v)

    def _emit_event(
        self,
        sym: str,
        event: str,
        side: str,
        price: float,
        plan: dict[str, Any],
        bar_t: int,
        v: list[Any],
        close_pct: Optional[int] = None,
        new_sl: Optional[float] = None,
        extra_payload: Optional[dict[str, Any]] = None,
        source_label: str = "TradingView WebSocket (PSL AI)",
    ) -> Any:
        payload: dict[str, Any] = {
            "key": self.secret_key,
            "event": event,
            "symbol": sym,
            "exchange": "BYBIT",
            "tf": self.tv_interval,
            "side": side,
            "trade_id": plan["trade_id"],
            "time": int(time.time() * 1000),
            "price": price,
        }
        if event == "entry":
            payload.update(
                {
                    "entry": plan["entry"],
                    "tp1": plan["tp1"],
                    "tp2": plan["tp2"],
                    "tp3": plan["tp3"],
                    "tp4": plan["tp4"],
                    "sl": plan["sl"],
                    "leverage": self.default_leverage,
                    "close_pct": _tf_close_pct_list(self.tv_interval),
                }
            )
        elif event.startswith("tp"):
            tp_num = int(event[-1])
            default_pct = _tf_close_pct_list(self.tv_interval)[tp_num - 1]
            pct_val = 100 if event == "tp4" else (default_pct if close_pct is None else close_pct)
            payload.update(
                {
                    "close_pct": pct_val,
                    "sl": None if event == "tp4" else (new_sl or plan["sl"]),
                }
            )
        elif event in ("stop", "liquidation"):
            pnl_val = _clean_num(self._plot(v, 8)) or 0.0
            payload.update({"result": event, "pnl": pnl_val})

        if extra_payload:
            payload.update(extra_payload)

        logger.info("Sinyal tetiklendi (%s): %s %s (%s)", source_label, event.upper(), sym, side)
        try:
            return self._signal_callback(payload, source_label)
        except Exception as exc:
            logger.exception("Sinyal işlenirken hata: %s", exc)
            return None

    # -----------------------------------------------------------------------
    # Doğrudan Bybit 5m Quant Dual-Alpha Motoru (ST20_M6 + SQZ36_ADX22 + BTC)
    # -----------------------------------------------------------------------
    def _fetch_bybit_klines_sync(
        self, rest_symbol: str, interval: str, limit: int = 650
    ) -> tuple[list[dict[str, float]], Optional[dict[str, float]]]:
        """Bybit V5 public kline verisini çeker ve kapalı mumlar + açık mumu ayırır."""
        byb_tf = _interval_to_bybit(interval)
        bar_ms = _interval_ms(interval)
        now_ms = int(time.time() * 1000)
        url = f"{self.bybit_api_base}/v5/market/kline"
        resp = self._http.get(
            url,
            params={"category": "linear", "symbol": rest_symbol, "interval": byb_tf, "limit": limit},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12,
        )
        resp.raise_for_status()
        data = resp.json()
        if int(data.get("retCode", -1)) != 0:
            raise RuntimeError(f"Bybit kline hatası ({rest_symbol}): {data.get('retMsg')}")

        rows = (data.get("result") or {}).get("list") or []
        closed: list[dict[str, float]] = []
        forming: Optional[dict[str, float]] = None
        for row in rows:
            ts = int(row[0])
            bar = {
                "t": ts,
                "o": float(row[1]),
                "h": float(row[2]),
                "l": float(row[3]),
                "c": float(row[4]),
                "v": float(row[5]),
            }
            if ts + bar_ms > now_ms:
                if forming is None or ts > forming["t"]:
                    forming = bar
            else:
                closed.append(bar)
        closed.sort(key=lambda x: x["t"])
        return closed, forming

    def _run_quant_loop(self) -> None:
        mode_disp = "Velez PCT Ultra" if self.quant_mode == "VELEZ_ULTRA" else f"Quant {self.quant_mode}"
        logger.info(
            "%s (%s | %s) motoru başlatılıyor: %s",
            mode_disp,
            self.quant_mode,
            self.tv_interval,
            ", ".join(self.tv_symbols),
        )
        self._update_status(running=True, state=f"{mode_disp} başlatılıyor...")
        poll_interval = max(8.0, self.poll_sec)

        while not self._stop_event.is_set():
            try:
                self._quant_step()
            except Exception as exc:
                logger.warning("Quant döngüsü uyarısı: %s", exc)
                self._update_status(last_error=str(exc))
            self._stop_event.wait(poll_interval)

        self._update_status(running=False, state="Durduruldu")

    def _quant_step(self) -> None:
        btc_closed, btc_forming = self._fetch_bybit_klines_sync("BTCUSDT", self.tv_interval, limit=650)
        if len(btc_closed) < 250:
            raise RuntimeError(f"BTCUSDT mum sayısı yetersiz ({len(btc_closed)})")

        btc_closes = [b["c"] for b in btc_closed]
        btc_ema200 = _calc_ema(btc_closes, 200)
        btc_close = btc_closes[-1]
        btc_e200 = btc_ema200[-1]
        btc_bull = btc_close > btc_e200
        btc_bear = btc_close < btc_e200

        for sym in self.tv_symbols:
            if self._stop_event.is_set():
                break
            state = self._coin_states[sym]
            rest_sym = state["bybit_rest_symbol"]
            try:
                if rest_sym == "BTCUSDT":
                    closed, forming = btc_closed, btc_forming
                else:
                    closed, forming = self._fetch_bybit_klines_sync(rest_sym, self.tv_interval, limit=650)
                if len(closed) < 250:
                    continue
                self._evaluate_quant_symbol(sym, state, closed, forming, btc_bull, btc_bear, btc_close, btc_e200)
            except Exception as exc:
                logger.warning("Quant sembol tarama hatası (%s): %s", sym, exc)
                state["last_error"] = str(exc)

        mode_disp = "Velez PCT Ultra" if self.quant_mode == "VELEZ_ULTRA" else f"Quant {self.quant_mode}"
        regime_txt = "🟢 BOĞA (BTC > EMA200)" if btc_bull else ("🔴 AYI (BTC < EMA200)" if btc_bear else "⚪ NÖTR")
        self._sync_coins_status(
            state_label=f"{mode_disp} ({len(self.tv_symbols)} coin · {regime_txt})"
        )

    def _evaluate_quant_symbol(
        self,
        sym: str,
        state: dict[str, Any],
        closed_bars: list[dict[str, float]],
        forming_bar: Optional[dict[str, float]],
        btc_bull: bool,
        btc_bear: bool,
        btc_close: float,
        btc_e200: float,
    ) -> None:
        n = len(closed_bars)
        closes = [b["c"] for b in closed_bars]
        vols = [b["v"] for b in closed_bars]
        atr14 = _calc_atr(closed_bars, 14)
        atr20 = _calc_atr(closed_bars, 20)
        ema50 = _calc_ema(closes, 50)
        ema200 = _calc_ema(closes, 200)
        sma20 = _calc_sma(closes, 20)
        sma200 = _calc_sma(closes, 200)
        sd20 = _calc_stdev(closes, sma20, 20)
        bb_up = [sma20[i] + 2.0 * sd20[i] for i in range(n)]
        bb_lo = [sma20[i] - 2.0 * sd20[i] for i in range(n)]
        ema20 = _calc_ema(closes, 20)
        kc_up = [ema20[i] + 1.5 * atr14[i] for i in range(n)]
        kc_lo = [ema20[i] - 1.5 * atr14[i] for i in range(n)]
        vol_sma20 = _calc_sma(vols, 20)
        adx14 = _calc_adx(closed_bars, 14)
        st_dir6, st_line6 = _calc_supertrend(closed_bars, atr20, 6.0)
        is_velez = self.quant_mode == "VELEZ_ULTRA"
        pct_long, pct_short, pct_meta = (
            _calc_velez_pct(closed_bars, sma20, sma200, atr14) if is_velez else (0, 0, {})
        )

        live_price = forming_bar["c"] if forming_bar else closes[-1]
        state["live_price"] = live_price
        state["indicator_detected"] = True
        state["needs_login_hint"] = False
        state["last_error"] = None
        state["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")

        last_closed_t = int(closed_bars[-1]["t"])

        # 1. Açık Pozisyon Takibi (Sert SL -> TP1/TP2 -> 1.2R Breakeven Kilidi)
        self._eval_quant_open_position(sym, state, closed_bars, forming_bar, live_price)

        # 2. Yeni Giriş Sinyali (Yalnızca YENİ kapanan mumda)
        sig_info: Optional[dict[str, Any]] = None
        if state["last_closed_ts"] is None:
            # Başlangıçta geçmiş mumdan geriye dönük sahte giriş açma; sıradaki yeni kapanışı bekle
            state["last_closed_ts"] = last_closed_t
        elif last_closed_t > state["last_closed_ts"]:
            state["last_closed_ts"] = last_closed_t
            sig_info = self._eval_quant_new_entry(
                sym, state, closed_bars, atr14, ema50, ema200,
                bb_up, bb_lo, kc_up, kc_lo, vol_sma20, adx14, st_dir6, st_line6,
                pct_long, pct_short, btc_bull, btc_bear, live_price,
            )

        # Web paneli için canlı özet tablo verisi
        c = closes[-1]
        st_lbl = "🟢 YUKARI" if st_dir6[-1] == 1 else "🔴 AŞAĞI"
        sqz_on = bb_up[-1] < kc_up[-1] and bb_lo[-1] > kc_lo[-1]
        btc_lbl = "BOĞA" if btc_bull else ("AYI" if btc_bear else "NÖTR")
        act = state.get("active_trade")
        plan_status = "active" if act else "waiting"
        plan_side = act["side"] if act else ("long" if st_dir6[-1] == 1 else "short")
        if is_velez:
            pct_tag = (
                f"AL {pct_long}/3"
                if pct_meta.get("pos_plus")
                else (f"SAT {pct_short}/3" if pct_meta.get("pos_minus") else "TUZAK 0/3")
            )
            verdict_text = f"{pct_tag} · BTC {btc_lbl}"
            raw_lines = [
                f"Motor: Velez PCT Ultra (Optimum ST6) | Zaman: {self.tv_interval}dk",
                f"Velez PCT: {pct_tag} | SMA20={sma20[-1]:.6g} / SMA200={sma200[-1]:.6g}",
                f"BTC Rejimi: {btc_lbl} (BTC={btc_close:.1f} / EMA200={btc_e200:.1f})",
                f"Supertrend(20,6): {st_lbl} (SL Hattı={st_line6[-1]:.6g}) | ADX={adx14[-1]:.1f} | Fiyat={c:.6g}",
            ]
        else:
            verdict_text = f"{'BOĞA' if st_dir6[-1] == 1 else 'AYI'} · BTC {btc_lbl}"
            raw_lines = [
                f"Quant Motor: {self.quant_mode} | Zaman: {self.tv_interval}dk",
                f"BTC Rejimi: {btc_lbl} (BTC={btc_close:.1f} / EMA200={btc_e200:.1f})",
                f"Supertrend(20,6): {st_lbl} (Çizgi={st_line6[-1]:.6g}) | EMA200={ema200[-1]:.6g}",
                f"ADX(14): {adx14[-1]:.1f} | BB Squeeze: {'AKTİF ⏳' if sqz_on else 'AÇIK'} | Fiyat={c:.6g}",
            ]
        state["parsed_table"] = {
            "indicator_detected": True,
            "verdict": verdict_text,
            "plan_side": plan_side,
            "plan_status": plan_status,
            "plan_status_text": f"🟢 AKTİF ({act.get('engine')})" if act else "⏳ Sinyal Bekleniyor",
            "entry": act["entry"] if act else sig_info.get("entry") if sig_info else None,
            "tp1": act["tp1"] if act else sig_info.get("tp1") if sig_info else None,
            "tp2": act["tp2"] if act else sig_info.get("tp2") if sig_info else None,
            "tp3": None,
            "tp4": None,
            "sl": act["sl"] if act else st_line6[-1],
            "checked_tps": list(act.get("hit_tps") or []) if act else [],
            "raw_lines": raw_lines,
        }

    def _eval_quant_open_position(
        self,
        sym: str,
        state: dict[str, Any],
        closed_bars: list[dict[str, float]],
        forming_bar: Optional[dict[str, float]],
        live_price: float,
    ) -> None:
        active = state.get("active_trade")
        if active is None:
            return

        sig_bar_t = int(active.get("signal_bar_t") or 0)
        chk_t = int(active.get("checked_through_t") or 0)
        check_bars = [b for b in closed_bars if b["t"] > sig_bar_t and b["t"] >= chk_t]
        if forming_bar is not None and forming_bar["t"] > sig_bar_t and forming_bar["t"] >= chk_t:
            check_bars.append(forming_bar)

        a_side = active["side"]
        prefix = "Velez PCT Ultra" if self.quant_mode == "VELEZ_ULTRA" else "Bybit Quant"
        engine_tag = f"{prefix} ({active.get('engine', self.quant_mode)})"
        for bar in check_bars:
            if state.get("active_trade") is None:
                return
            bar_t_sec = int(bar["t"] // 1000)
            is_closed = (forming_bar is None) or (bar["t"] < forming_bar["t"])
            sl_px = float(active["sl"])

            # 1) Hard SL kontrolü (BE yeni kilitlendiyse aynı mumun eski fitiline değil canlı/kapanış fiyatına bak)
            if active.get("be_locked_bar_t") == int(bar["t"]):
                chk_px = bar["c"] if is_closed else live_price
                sl_hit = (a_side == "long" and chk_px <= sl_px) or (a_side == "short" and chk_px >= sl_px)
            else:
                sl_hit = (a_side == "long" and bar["l"] <= sl_px) or (a_side == "short" and bar["h"] >= sl_px)
            if sl_hit:
                self._emit_event(
                    sym, "stop", a_side, live_price or sl_px, active, bar_t_sec, [],
                    extra_payload={"engine": active.get("engine")},
                    source_label=engine_tag,
                )
                state["active_trade"] = None
                return

            # 2) TP1 (%50 Kâr Al + SL -> Giriş+%0.20 BE)
            tp1_hit_now = False
            if "tp1" not in active["hit_tps"]:
                t1_hit = (a_side == "long" and bar["h"] >= active["tp1"]) or (a_side == "short" and bar["l"] <= active["tp1"])
                if t1_hit:
                    tp1_hit_now = True
                    active["hit_tps"].append("tp1")
                    active["be_locked"] = True
                    active["be_locked_bar_t"] = int(bar["t"])
                    be_sl = active["entry"] * (1.0020 if a_side == "long" else 0.9980)
                    active["sl"] = be_sl
                    self._emit_event(
                        sym, "tp1", a_side, active["tp1"], active, bar_t_sec, [],
                        close_pct=50, new_sl=be_sl,
                        extra_payload={"engine": active.get("engine")},
                        source_label=engine_tag,
                    )

            # 3) TP2 (Kalan %50 = Tam Kapanış)
            if "tp1" in active["hit_tps"] and "tp2" not in active["hit_tps"]:
                t2_hit = (a_side == "long" and bar["h"] >= active["tp2"]) or (a_side == "short" and bar["l"] <= active["tp2"])
                if t2_hit:
                    active["hit_tps"].append("tp2")
                    self._emit_event(
                        sym, "tp4", a_side, active["tp2"], active, bar_t_sec, [],
                        close_pct=100, new_sl=None,
                        extra_payload={"engine": active.get("engine")},
                        source_label=engine_tag,
                    )
                    state["active_trade"] = None
                    return

            # 4) 1.2R Erken Breakeven Kilidi (%0 kapatma, SL -> Giriş+%0.20)
            be_hit_now = False
            if "tp1" not in active["hit_tps"] and not active.get("be_locked"):
                be_trig = float(active.get("be_trigger") or 0.0)
                if be_trig > 0 and ((a_side == "long" and bar["h"] >= be_trig) or (a_side == "short" and bar["l"] <= be_trig)):
                    be_hit_now = True
                    active["be_locked"] = True
                    active["be_locked_bar_t"] = int(bar["t"])
                    be_sl = active["entry"] * (1.0020 if a_side == "long" else 0.9980)
                    active["sl"] = be_sl
                    self._emit_event(
                        sym, "tp1", a_side, live_price, active, bar_t_sec, [],
                        close_pct=0, new_sl=be_sl,
                        extra_payload={"engine": active.get("engine")},
                        source_label=engine_tag,
                    )

            # Muhafazakâr kapanış kontrolü: aynı kapalı mumda BE/TP1 görülüp kapanışta BE delindiyse kapat
            if is_closed and (tp1_hit_now or be_hit_now):
                be_broken = (a_side == "long" and bar["c"] <= active["sl"]) or (a_side == "short" and bar["c"] >= active["sl"])
                if be_broken:
                    self._emit_event(
                        sym, "stop", a_side, bar["c"], active, bar_t_sec, [],
                        extra_payload={"engine": active.get("engine")},
                        source_label=engine_tag,
                    )
                    state["active_trade"] = None
                    return

            active["checked_through_t"] = int(bar["t"])

    def _eval_quant_new_entry(
        self,
        sym: str,
        state: dict[str, Any],
        closed_bars: list[dict[str, float]],
        atr14: list[float],
        ema50: list[float],
        ema200: list[float],
        bb_up: list[float],
        bb_lo: list[float],
        kc_up: list[float],
        kc_lo: list[float],
        vol_sma20: list[float],
        adx14: list[float],
        st_dir6: list[int],
        st_line6: list[float],
        pct_long: int,
        pct_short: int,
        btc_bull: bool,
        btc_bear: bool,
        live_price: float,
    ) -> Optional[dict[str, Any]]:
        if state.get("active_trade") is not None:
            return None

        n = len(closed_bars)
        b = closed_bars[-1]
        c = b["c"]
        if c <= 0 or atr14[-1] <= 0:
            return None

        picked: Optional[dict[str, Any]] = None

        if self.quant_mode == "VELEZ_ULTRA":
            # Optimum Velez PCT Ultra: Supertrend(20, 6.0) + Velez PCT >= 2/3 + BTC EMA200 (Sıkışma fakeout'ları kapalı)
            bar_ms = _interval_ms(self.tv_interval)
            min_sl_pct = 0.012 if bar_ms <= 300_000 else (0.015 if bar_ms <= 900_000 else 0.020)
            max_sl_pct = 0.030 if bar_ms <= 300_000 else (0.035 if bar_ms <= 900_000 else 0.050)
            if st_dir6[-1] != st_dir6[-2]:
                sl_dist = min(max(abs(c - st_line6[-1]), c * min_sl_pct), c * max_sl_pct)
                if st_dir6[-1] == 1 and pct_long >= 2 and btc_bull:
                    picked = {"side": "long", "sl_dist": sl_dist, "tp1_r": 2.0, "tp2_r": 4.0, "engine": f"VELEZ_ST6_{pct_long}/3"}
                elif st_dir6[-1] == -1 and pct_short >= 2 and btc_bear:
                    picked = {"side": "short", "sl_dist": sl_dist, "tp1_r": 2.0, "tp2_r": 4.0, "engine": f"VELEZ_ST6_{pct_short}/3"}
        else:
            # Motor 1: Geniş Supertrend(20, 6.0) + EMA200 + BTC Rejim Filtresi
            if st_dir6[-1] != st_dir6[-2]:
                sl_dist = min(max(abs(c - st_line6[-1]), c * 0.012), c * 0.030)
                if st_dir6[-1] == 1 and c > ema200[-1] and btc_bull:
                    picked = {"side": "long", "sl_dist": sl_dist, "tp1_r": 2.0, "tp2_r": 4.0, "engine": "ST20_M6"}
                elif st_dir6[-1] == -1 and c < ema200[-1] and btc_bear:
                    picked = {"side": "short", "sl_dist": sl_dist, "tp1_r": 2.0, "tp2_r": 4.0, "engine": "ST20_M6"}

            # Motor 2: BB/KC Sıkışma Patlaması + ADX>=22 + Hacim>=1.8x + 36-bar Kırılım + BTC Rejim
            if picked is None and self.quant_mode != "ST_ONLY" and vol_sma20[-1] > 0:
                sqz_prev = any(bb_up[k] < kc_up[k] and bb_lo[k] > kc_lo[k] for k in range(n - 9, n - 1))
                released = not (bb_up[-1] < kc_up[-1] and bb_lo[-1] > kc_lo[-1])
                if sqz_prev and released and b["v"] >= 1.8 * vol_sma20[-1] and adx14[-1] >= 22.0:
                    hi36 = max(closed_bars[k]["h"] for k in range(n - 37, n - 1))
                    lo36 = min(closed_bars[k]["l"] for k in range(n - 37, n - 1))
                    sl_dist = min(max(2.0 * atr14[-1], c * 0.012), c * 0.025)
                    if btc_bull and c > bb_up[-1] and c > hi36 and c > ema50[-1] > ema200[-1]:
                        picked = {"side": "long", "sl_dist": sl_dist, "tp1_r": 1.8, "tp2_r": 3.6, "engine": "SQZ36_ADX22"}
                    elif btc_bear and c < bb_lo[-1] and c < lo36 and c < ema50[-1] < ema200[-1]:
                        picked = {"side": "short", "sl_dist": sl_dist, "tp1_r": 1.8, "tp2_r": 3.6, "engine": "SQZ36_ADX22"}

        if picked is None:
            return None

        side = picked["side"]
        sd = picked["sl_dist"]
        entry_px = live_price or c
        if side == "long":
            sl_p = entry_px - sd
            be_trig = entry_px + sd * 1.2
            tp1_p = entry_px + sd * picked["tp1_r"]
            tp2_p = entry_px + sd * picked["tp2_r"]
        else:
            sl_p = entry_px + sd
            be_trig = entry_px - sd * 1.2
            tp1_p = entry_px - sd * picked["tp1_r"]
            tp2_p = entry_px - sd * picked["tp2_r"]

        self._trade_seq += 1
        bar_t_sec = int(b["t"] // 1000)
        plan = {
            "trade_id": self._trade_seq,
            "side": side,
            "engine": picked["engine"],
            "entry": entry_px,
            "tp1": tp1_p,
            "tp2": tp2_p,
            "tp3": tp2_p,
            "tp4": tp2_p,
            "sl": sl_p,
            "be_trigger": be_trig,
            "be_locked": False,
            "hit_tps": [],
            "signal_bar_t": int(b["t"]),
            "checked_through_t": int(b["t"]),
            "opened_candle_ms": int(b["t"]),
        }
        state["active_trade"] = plan
        src_prefix = "Velez PCT Ultra" if self.quant_mode == "VELEZ_ULTRA" else "Bybit Quant"
        res = self._emit_event(
            sym, "entry", side, entry_px, plan, bar_t_sec, [],
            extra_payload={
                "hard_sl": True,
                "close_pct": [50, 50, 0, 0],
                "leverage": _tf_sim_leverage(self.tv_interval, self.default_leverage),
                "engine": picked["engine"],
            },
            source_label=f"{src_prefix} ({picked['engine']})",
        )
        if isinstance(res, dict) and res.get("status") in ("skipped", "rejected"):
            logger.info("%s girişi bot tarafından atlandı (%s), aktif takip temizlendi.", sym, res.get("reason"))
            state["active_trade"] = None
            return None
        return plan
