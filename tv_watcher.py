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
import random
import re
import string
import threading
import time
from typing import Any, Callable, Optional

import aiohttp
import requests

logger = logging.getLogger("psl-tv-watcher")

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
    """PSL AI zaman dilimine özel TP1-TP4 kapanış yüzdelerini döndürür."""
    norm = _normalize_interval(interval)
    if norm == "5":
        return [0, 0, 50, 50]
    if norm in ("1", "15", "60", "240"):
        return [0, 0, 0, 100]
    return [25, 25, 25, 25]


def parse_psl_table(lines: list[str]) -> dict[str, Any]:
    """TradingView tablo hücrelerinden PSL AI v2.1 panel verilerini ayıklar."""
    result: dict[str, Any] = {
        "indicator_detected": False,
        "verdict": "",
        "plan_side": "",
        "plan_status": "unknown",
        "plan_status_text": "",
        "last_sim_trade": "",
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
            elif any(w in header_window for w in ("TETİK BEKLİYOR", "AWAITING TRIGGER", "BEKLİYOR", "WAITING")):
                result["plan_status"] = "waiting"
                result["plan_status_text"] = "TETİK BEKLİYOR"
            elif any(w in header_window for w in ("AKTİF", "ACTIVE")):
                result["plan_status"] = "active"
                result["plan_status_text"] = "AKTİF"
            elif any(w in header_window for w in ("HEDEFLERE ULAŞILDI", "TARGETS REACHED", "TAMAMLANDI")):
                result["plan_status"] = "completed"
                result["plan_status_text"] = "HEDEFLERE ULAŞILDI"
            elif any(w in header_window for w in ("GEÇERSİZ OLDU", "STOPPED", "KAPANDI", "CLOSED")):
                result["plan_status"] = "stopped"
                result["plan_status_text"] = "KAPANDI"
            elif "—" in header_window or "| -" in header_window:
                result["plan_status"] = "none"
                result["plan_status_text"] = "Plan Bekleniyor (—)"

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

        if result["entry"] is None:
            val = _extract_first_price(uline, r"\b(?:GİRİŞ|ENTRY)\b")
            if val is not None:
                result["entry"] = val

        for tp_idx in range(1, 5):
            tp_key = f"tp{tp_idx}"
            if result[tp_key] is None:
                val = _extract_first_price(uline, rf"\b(?:HEDEF\s*{tp_idx}|TARGET\s*{tp_idx}|TP\s*{tp_idx})\b")
                if val is not None:
                    result[tp_key] = val

        if result["sl"] is None:
            val = _extract_first_price(uline, r"\b(?:GEÇERSİZ|INVALIDATION|STOP)\b")
            if val is not None:
                result["sl"] = val

    if result["last_sim_trade"]:
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
        self.tv_interval = _normalize_interval(tv_interval or "15")
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
        return {
            "text": inputs["text"],
            "pineId": meta["scriptIdPart"],
            "pineVersion": (meta.get("pine") or {}).get("version", "3.0"),
            "in_27": {"v": 100, "f": True, "t": "integer"},
            "in_28": {"v": 15, "f": True, "t": "integer"},
            "in_29": {"v": 50, "f": True, "t": "integer"},
            "in_39": {"v": True, "f": True, "t": "bool"},
        }

    def _run_loop(self) -> None:
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
                                    self._sync_coins_status()
                                    continue

                                if m_type != "du" or len(params) < 2 or not isinstance(params[1], dict):
                                    continue

                                payload_dict = params[1]

                                # Anlık mum fiyatını güncelle (sds_1)
                                sds_data = payload_dict.get("sds_1")
                                if isinstance(sds_data, dict):
                                    bars = sds_data.get("s") or []
                                    if bars:
                                        last_v = bars[-1].get("v") or []
                                        if len(last_v) >= 5:
                                            close_p = _clean_num(last_v[4])
                                            if close_p is not None:
                                                state["live_price"] = close_p

                                # PSL AI study verisini işle (st1)
                                st_data = payload_dict.get("st1")
                                if not isinstance(st_data, dict):
                                    continue

                                state["indicator_detected"] = True
                                state["needs_login_hint"] = False
                                state["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")

                                ns_d = (st_data.get("ns") or {}).get("d")
                                table_lines = _extract_table_lines_from_ns(ns_d)
                                prev_parsed = state.get("parsed_table") or {}
                                parsed_table = dict(prev_parsed)
                                if table_lines:
                                    parsed_table = parse_psl_table(table_lines)
                                    if not parsed_table.get("last_sim_trade") and prev_parsed.get("last_sim_trade"):
                                        parsed_table["last_sim_trade"] = prev_parsed["last_sim_trade"]
                                        parsed_table["plan_status_text"] = prev_parsed["last_sim_trade"]

                                bars = st_data.get("st") or st_data.get("s") or []
                                for bar in bars:
                                    v = bar.get("v") or []
                                    if len(v) < 7:
                                        continue
                                    bar_t_raw = _clean_num(v[0])
                                    if bar_t_raw is None:
                                        continue
                                    bar_t = int(bar_t_raw)

                                    # Mum kapanışı kontrolü: yeni bar timestamp'i geldiyse önceki bar kapandı demektir
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

                                    # Anlık (intrabar) olaylar: TP1-4 ve Likidasyon + Entry fallback
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
        Grafik ortasındaki gerçek işlem plotlarını her zaman sol tablodan öncelikli tutar.
        """
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
    ) -> None:
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

        logger.info("TradingView WebSocket sinyali yakalandı: %s %s (%s)", event.upper(), sym, side)
        try:
            self._signal_callback(payload, "TradingView WebSocket (PSL AI)")
        except Exception as exc:
            logger.exception("WebSocket sinyali işlenirken hata: %s", exc)
