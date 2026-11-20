"""
PSL AI v2.1 -> Bybit Futures Otomatik Al-Sat Botu
- Ücretsiz TradingView Hesaplar için Bulut Görünmez Tarayıcı (Playwright) Tablo Okuyucusu
- Gmail IMAP IDLE (Anlık Push E-posta) Okuyucu (Opsiyonel)
- Ücretli/Deneme Hesaplar veya Köprüler için FastAPI Webhook Endpoint
- Bybit V5 USDT Perpetual (Futures) Emir Motoru (ccxt)
- Telegram Bildirim ve Canlı Türkçe Web Paneli
"""

from __future__ import annotations

import email
import html
import json
import logging
import os
import re
import threading
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Any, Optional

import ccxt
import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from imapclient import IMAPClient

from tv_watcher import TradingViewPlaywrightWatcher

load_dotenv()

# ---------------------------------------------------------------------------
# Loglama Ayarları
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("psl-ai-bot")


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on", "evet")


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if not val:
        return default
    try:
        return float(val.strip())
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if not val:
        return default
    try:
        return int(val.strip())
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Yapılandırma
# ---------------------------------------------------------------------------
class Config:
    # Bybit API
    BYBIT_API_KEY: str = os.getenv("BYBIT_API_KEY", "").strip()
    BYBIT_API_SECRET: str = os.getenv("BYBIT_API_SECRET", "").strip()
    BYBIT_TESTNET: bool = _env_bool("BYBIT_TESTNET", True)
    BYBIT_HEDGE_MODE: bool = _env_bool("BYBIT_HEDGE_MODE", False)

    # Risk & Pozisyon Ayarları
    DRY_RUN: bool = _env_bool("DRY_RUN", False)
    MARGIN_USDT: float = _env_float("MARGIN_USDT", 15.0)
    DEFAULT_LEVERAGE: int = _env_int("DEFAULT_LEVERAGE", 50)
    MAX_LEVERAGE: int = _env_int("MAX_LEVERAGE", 50)
    AUTO_TF_LEVERAGE: bool = _env_bool("AUTO_TF_LEVERAGE", True)
    EMERGENCY_HARD_SL_PCT: float = _env_float("EMERGENCY_HARD_SL_PCT", 0.0)

    # Güvenlik & Filtreler
    PSL_SECRET_KEY: str = os.getenv("PSL_SECRET_KEY", "").strip()
    ALLOWED_SYMBOLS: set[str] = {
        s.strip().upper()
        for s in os.getenv("ALLOWED_SYMBOLS", "").split(",")
        if s.strip()
    }
    MAX_EMAIL_AGE_SEC: int = _env_int("MAX_EMAIL_AGE_SEC", 180)

    # Bulut Görünmez Tarayıcı (Playwright - Ücretsiz TradingView Çözümü)
    ENABLE_PLAYWRIGHT: bool = _env_bool("ENABLE_PLAYWRIGHT", True)
    TV_CHART_URL: str = os.getenv("TV_CHART_URL", "").strip()
    TV_SYMBOL: str = os.getenv("TV_SYMBOL", "BYBIT:BTCUSDT.P").strip()
    TV_INTERVAL: str = os.getenv("TV_INTERVAL", "15").strip()
    TV_SESSIONID: str = os.getenv("TV_SESSIONID", "").strip()
    TV_SESSIONID_SIGN: str = os.getenv("TV_SESSIONID_SIGN", "").strip()
    TV_POLL_SEC: float = _env_float("TV_POLL_SEC", 2.0)

    # Gmail IMAP IDLE (Opsiyonel E-posta Dinleyici)
    ENABLE_IMAP: bool = _env_bool("ENABLE_IMAP", False)
    IMAP_HOST: str = os.getenv("IMAP_HOST", "imap.gmail.com").strip()
    IMAP_PORT: int = _env_int("IMAP_PORT", 993)
    IMAP_FOLDER: str = os.getenv("IMAP_FOLDER", "INBOX").strip()
    GMAIL_ADDRESS: str = os.getenv("GMAIL_ADDRESS", "").strip()
    GMAIL_APP_PASSWORD: str = os.getenv("GMAIL_APP_PASSWORD", "").replace(" ", "").strip()
    ALLOWED_EMAIL_SENDER: str = os.getenv("ALLOWED_EMAIL_SENDER", "").strip().lower()

    # Telegram Bildirimleri
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    # Sunucu (Hugging Face Spaces varsayılan portu: 7860)
    HOST: str = os.getenv("HOST", "0.0.0.0").strip()
    PORT: int = _env_int("PORT", 7860)


# ---------------------------------------------------------------------------
# Yardımcı Araçlar (Telegram, Sembol Normalizasyonu, JSON Ayıklama)
# ---------------------------------------------------------------------------
def send_telegram(message: str) -> None:
    """Telegram üzerinden anlık işlem ve hata bildirimi gönderir."""
    if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        with httpx.Client(timeout=8.0) as client:
            client.post(
                url,
                json={
                    "chat_id": Config.TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
    except Exception as exc:
        logger.warning("Telegram bildirimi gönderilemedi: %s", exc)


def normalize_symbol(raw_symbol: str) -> str:
    """
    TradingView sembolünü (örn: BTCUSDT.P, BYBIT:ETHUSDT.P)
    CCXT Bybit Perpetual formatına (BTC/USDT:USDT) dönüştürür.
    """
    sym = raw_symbol.strip().upper()
    if ":" in sym:
        # Eğer zaten CCXT formatındaysa (BTC/USDT:USDT) dokunma
        if "/" in sym:
            return sym
        sym = sym.split(":")[-1]

    for suffix in (".P", ".PS", "PERP"):
        if sym.endswith(suffix):
            sym = sym[: -len(suffix)]

    if "/" in sym:
        base, quote = sym.split("/", 1)
        return f"{base}/{quote}:{quote}"

    for quote in ("USDT", "USDC", "USD"):
        if sym.endswith(quote) and len(sym) > len(quote):
            base = sym[: -len(quote)]
            return f"{base}/{quote}:{quote}"

    return sym


def strip_html(text: str) -> str:
    """HTML etiketlerini temizler ve karakter entity'lerini çözer."""
    cleaned = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    return html.unescape(cleaned)


def extract_psl_json(raw_text: str) -> Optional[dict[str, Any]]:
    """
    E-posta gövdesi veya konu satırı içindeki PSL AI v2.1 JSON verisini bulur.
    HTML sarmalayıcıları veya etraftaki TradingView metinlerini güvenle atlar.
    """
    if not raw_text:
        return None

    candidates = [raw_text, html.unescape(raw_text), strip_html(raw_text)]
    decoder = json.JSONDecoder()

    for text in candidates:
        idx = 0
        while idx < len(text):
            brace_pos = text.find("{", idx)
            if brace_pos == -1:
                break
            try:
                obj, end_pos = decoder.raw_decode(text, brace_pos)
                if isinstance(obj, dict) and "event" in obj and "symbol" in obj:
                    return obj
                idx = end_pos
            except json.JSONDecodeError:
                idx = brace_pos + 1
    return None


def decode_mime_header(value: Optional[str]) -> str:
    """MIME kodlanmış e-posta başlıklarını UTF-8 metne çevirir."""
    if not value:
        return ""
    parts = decode_header(value)
    decoded: list[str] = []
    for chunk, charset in parts:
        if isinstance(chunk, bytes):
            decoded.append(chunk.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(chunk)
    return "".join(decoded)


def extract_email_text(msg: email.message.Message) -> str:
    """E-postanın konu, düz metin ve HTML bölümlerini birleştirerek okur."""
    chunks: list[str] = []
    subject = decode_mime_header(msg.get("Subject"))
    if subject:
        chunks.append(subject)

    if msg.is_multipart():
        plain_parts: list[str] = []
        html_parts: list[str] = []
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "")
            if "attachment" in disposition.lower():
                continue
            payload = part.get_payload(decode=True)
            if not isinstance(payload, bytes):
                continue
            charset = part.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            if content_type == "text/plain":
                plain_parts.append(decoded)
            elif content_type == "text/html":
                html_parts.append(decoded)
        # Önce düz metin bölümlerini dene, ardından HTML
        chunks.extend(plain_parts)
        chunks.extend(html_parts)
    else:
        payload = msg.get_payload(decode=True)
        if isinstance(payload, bytes):
            charset = msg.get_content_charset() or "utf-8"
            chunks.append(payload.decode(charset, errors="replace"))

    return "\n".join(chunks)


# ---------------------------------------------------------------------------
# Tekrar Önleyici (Deduplication) ve Durum Yönetimi
# ---------------------------------------------------------------------------
class SignalDeduplicator:
    """Aynı sinyalin çift işlenmesini engeller."""

    def __init__(self, max_size: int = 2000) -> None:
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._max_size = max_size
        self._lock = threading.Lock()

    def is_duplicate(self, payload: dict[str, Any]) -> bool:
        key_parts = [
            str(payload.get("symbol", "")).upper(),
            str(payload.get("trade_id", "")),
            str(payload.get("event", "")).lower(),
            str(payload.get("time", "")),
        ]
        dedup_key = "|".join(key_parts)
        with self._lock:
            if dedup_key in self._seen:
                return True
            self._seen[dedup_key] = time.time()
            while len(self._seen) > self._max_size:
                self._seen.popitem(last=False)
            return False


# ---------------------------------------------------------------------------
# Bybit V5 Futures Emir Motoru
# ---------------------------------------------------------------------------
class BybitTrader:
    """PSL AI v2.1 sinyallerini Bybit USDT Perpetual işlemlerine dönüştürür."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_trades: dict[str, dict[str, Any]] = {}
        self._exchange: Optional[ccxt.bybit] = None
        self.account_mode_name: str = "DRY-RUN"
        self.usdt_balance: Optional[float] = None
        self.connected: bool = False

        if Config.DRY_RUN:
            logger.info("DRY_RUN (Simülasyon) modu aktif — Bybit'e gerçek emir gitmeyecek.")
            return

        if not Config.BYBIT_API_KEY or not Config.BYBIT_API_SECRET:
            raise RuntimeError(
                "DRY_RUN=false iken BYBIT_API_KEY ve BYBIT_API_SECRET zorunludur!"
            )

        modes = (
            [("demo", "DEMO HESAP"), ("testnet", "TESTNET"), ("mainnet", "GERÇEK HESAP (CANLI)")]
            if Config.BYBIT_TESTNET
            else [("mainnet", "GERÇEK HESAP (CANLI)"), ("demo", "DEMO HESAP")]
        )

        last_err: Optional[Exception] = None
        for mode_key, mode_label in modes:
            client = self._build_client(mode_key)
            try:
                bal = self._fetch_unified_usdt_balance(client)
                self._exchange = client
                self.account_mode_name = mode_label
                self.usdt_balance = bal
                self.connected = True
                logger.info("Bybit bağlantısı başarılı: %s | Bakiye: %.2f USDT", mode_label, bal)
                break
            except Exception as exc:
                last_err = exc
                err_txt = str(exc)
                if "10003" in err_txt or "API key is invalid" in err_txt:
                    logger.debug("Bybit %s modunda 10003 alındı, diğer mod deneniyor...", mode_key)
                    continue
                logger.warning("Bybit %s bağlantı denemesi uyarısı: %s", mode_key, exc)

        if self._exchange is None:
            logger.error("Bybit kimlik doğrulaması başarısız: %s", last_err)
            return

        try:
            self._exchange.load_markets()
            logger.info("Bybit market bilgileri başarıyla yüklendi.")
        except Exception as exc:
            logger.error("Bybit marketleri yüklenemedi: %s", exc)

    def _build_client(self, mode_key: str) -> ccxt.bybit:
        client = ccxt.bybit(
            {
                "apiKey": Config.BYBIT_API_KEY,
                "secret": Config.BYBIT_API_SECRET,
                "enableRateLimit": True,
                "options": {
                    "defaultType": "swap",
                    "adjustForTimeDifference": True,
                },
            }
        )
        if mode_key == "demo":
            client.enable_demo_trading(True)
        elif mode_key == "testnet":
            client.set_sandbox_mode(True)
        return client

    @staticmethod
    def _fetch_unified_usdt_balance(client: ccxt.bybit) -> float:
        resp = client.private_get_v5_account_wallet_balance({"accountType": "UNIFIED"})
        accounts = ((resp.get("result") or {}).get("list")) or []
        for acc in accounts:
            for coin_info in acc.get("coin") or []:
                if str(coin_info.get("coin", "")).upper() == "USDT":
                    val = coin_info.get("walletBalance") or coin_info.get("equity") or 0
                    return float(val)
            total_eq = acc.get("totalEquity") or acc.get("totalWalletBalance")
            if total_eq is not None:
                return float(total_eq)
        return 0.0

    def refresh_balance(self) -> Optional[float]:
        """Bybit Unified cüzdandaki güncel USDT bakiyesini yeniler."""
        if Config.DRY_RUN or self._exchange is None:
            return self.usdt_balance
        try:
            self.usdt_balance = self._fetch_unified_usdt_balance(self._exchange)
        except Exception as exc:
            logger.debug("Bakiye yenilenemedi: %s", exc)
        return self.usdt_balance

    def _position_idx(self, side: str) -> int:
        if not Config.BYBIT_HEDGE_MODE:
            return 0
        return 1 if side.lower() == "long" else 2

    def _set_leverage(self, symbol: str, leverage: int) -> None:
        if Config.DRY_RUN or self._exchange is None:
            return
        try:
            self._exchange.set_leverage(leverage, symbol)
        except ccxt.BaseError as exc:
            # Bybit kaldıraç zaten aynıysa hata kodu 110043 döner; güvenle yutulur
            msg = str(exc)
            if "110043" in msg or "leverage not modified" in msg.lower():
                return
            logger.warning("%s için kaldıraç (%sx) ayarlama uyarısı: %s", symbol, leverage, exc)

    def _get_open_position(self, symbol: str, side: str) -> float:
        """Borsadaki mevcut açık pozisyon kontrat/coin miktarını döndürür."""
        if Config.DRY_RUN or self._exchange is None:
            trade = self._active_trades.get(symbol)
            return float(trade.get("remaining_qty", 0.0)) if trade else 0.0

        positions = self._exchange.fetch_positions([symbol])
        target_side = side.lower()
        for pos in positions:
            pos_symbol = pos.get("symbol")
            contracts = float(pos.get("contracts") or 0.0)
            pos_side = str(pos.get("side") or "").lower()
            if pos_symbol == symbol and contracts > 0:
                if not pos_side or pos_side == target_side:
                    return contracts
        return 0.0

    def _get_ref_price(self, symbol: str, fallback_price: float) -> float:
        if Config.DRY_RUN or self._exchange is None:
            return fallback_price
        try:
            ticker = self._exchange.fetch_ticker(symbol)
            last = ticker.get("last") or ticker.get("mark")
            if last:
                return float(last)
        except Exception as exc:
            logger.warning("Fiyat çekilemedi, sinyal fiyatı kullanılıyor (%s): %s", symbol, exc)
        return fallback_price

    def _format_amount(self, symbol: str, amount: float) -> float:
        if Config.DRY_RUN or self._exchange is None:
            return round(amount, 6)
        formatted = float(self._exchange.amount_to_precision(symbol, amount))
        return formatted

    def _place_market_order(
        self,
        symbol: str,
        order_side: str,
        amount: float,
        position_side: str,
        reduce_only: bool = False,
        stop_loss: Optional[float] = None,
    ) -> dict[str, Any]:
        if Config.DRY_RUN or self._exchange is None:
            return {
                "id": f"dry_{int(time.time() * 1000)}",
                "status": "closed",
                "average": None,
                "filled": amount,
            }

        params: dict[str, Any] = {
            "positionIdx": self._position_idx(position_side),
        }
        if reduce_only:
            params["reduceOnly"] = True
        if stop_loss and stop_loss > 0:
            params["stopLoss"] = self._exchange.price_to_precision(symbol, stop_loss)

        return self._exchange.create_order(
            symbol=symbol,
            type="market",
            side=order_side,
            amount=amount,
            params=params,
        )

    def execute_signal(self, payload: dict[str, Any], source: str, latency_info: str) -> None:
        event = str(payload.get("event", "")).lower().strip()
        raw_symbol = str(payload.get("symbol", "")).strip()
        ccxt_symbol = normalize_symbol(raw_symbol)
        side = str(payload.get("side", "")).lower().strip()
        trade_id = payload.get("trade_id")

        if side not in ("long", "short"):
            logger.warning("Geçersiz yön (side=%s), sinyal atlandı: %s", side, payload)
            return

        if Config.ALLOWED_SYMBOLS:
            base_check = raw_symbol.upper().replace(".P", "")
            if raw_symbol.upper() not in Config.ALLOWED_SYMBOLS and base_check not in Config.ALLOWED_SYMBOLS:
                logger.info("Sembol izin listesinde değil (%s), sinyal atlandı.", raw_symbol)
                return

        with self._lock:
            if event == "entry":
                self._handle_entry(payload, ccxt_symbol, side, trade_id, source, latency_info)
            elif event in ("tp1", "tp2", "tp3", "tp4"):
                self._handle_tp(payload, event, ccxt_symbol, side, trade_id, source, latency_info)
            elif event in ("stop", "liquidation"):
                self._handle_close(payload, event, ccxt_symbol, side, trade_id, source, latency_info)
            else:
                logger.warning("Bilinmeyen PSL AI event türü: %s", event)

    def _resolve_leverage(self, payload: dict[str, Any], symbol: str) -> int:
        """İşlem kaldıracını belirler ve Bybit market üst sınırına göre kırpar."""
        if Config.AUTO_TF_LEVERAGE:
            tf = str(payload.get("tf") or Config.TV_INTERVAL).strip().lower()
            tf_table = {
                "1": 50,
                "1m": 50,
                "5": 50,
                "5m": 50,
                "15": 20,
                "15m": 20,
                "60": 15,
                "1h": 15,
                "240": 8,
                "4h": 8,
            }
            req_lev = tf_table.get(tf, Config.DEFAULT_LEVERAGE)
        else:
            req_lev = int(payload.get("leverage") or Config.DEFAULT_LEVERAGE)

        lev = max(1, min(req_lev, Config.MAX_LEVERAGE))
        if self._exchange and self._exchange.markets and symbol in self._exchange.markets:
            max_mkt = (
                ((self._exchange.markets[symbol].get("limits") or {}).get("leverage") or {}).get("max")
            )
            if max_mkt:
                lev = min(lev, int(float(max_mkt)))
        return max(1, lev)

    def _handle_entry(
        self,
        payload: dict[str, Any],
        symbol: str,
        side: str,
        trade_id: Any,
        source: str,
        latency_info: str,
    ) -> None:
        opposite_side = "short" if side == "long" else "long"
        opp_qty = self._get_open_position(symbol, opposite_side)
        if opp_qty > 0:
            close_order_side = "buy" if opposite_side == "short" else "sell"
            logger.info("Ters yön açık pozisyon kapatılıyor: %s %s (%s)", symbol, opposite_side, opp_qty)
            self._place_market_order(
                symbol=symbol,
                order_side=close_order_side,
                amount=opp_qty,
                position_side=opposite_side,
                reduce_only=True,
            )

        existing_qty = self._get_open_position(symbol, side)
        if existing_qty > 0:
            logger.warning(
                "%s için zaten %s pozisyon açık (miktar=%s). Çift giriş yapılmadı.",
                symbol,
                side,
                existing_qty,
            )
            return

        leverage = self._resolve_leverage(payload, symbol)
        self._set_leverage(symbol, leverage)

        signal_price = float(payload.get("entry") or payload.get("price") or 0.0)
        ref_price = self._get_ref_price(symbol, signal_price)
        if ref_price <= 0:
            logger.error("Geçerli fiyat bulunamadı (%s), giriş iptal.", symbol)
            return

        notional = Config.MARGIN_USDT * leverage
        raw_qty = notional / ref_price
        qty = self._format_amount(symbol, raw_qty)
        if qty <= 0:
            logger.error("Hesaplanan miktar 0 veya minimumun altında (%s, raw=%s).", symbol, raw_qty)
            return

        # İsteğe bağlı acil durum koruma stobu (varsayılan 0 = kapalı; mum kapanışı stop kuralı için)
        emergency_sl: Optional[float] = None
        if Config.EMERGENCY_HARD_SL_PCT > 0:
            pct = Config.EMERGENCY_HARD_SL_PCT / 100.0
            emergency_sl = ref_price * (1.0 - pct) if side == "long" else ref_price * (1.0 + pct)

        order_side = "buy" if side == "long" else "sell"
        order = self._place_market_order(
            symbol=symbol,
            order_side=order_side,
            amount=qty,
            position_side=side,
            reduce_only=False,
            stop_loss=emergency_sl,
        )

        close_pct = payload.get("close_pct")
        if not isinstance(close_pct, list) or len(close_pct) < 4:
            close_pct = [25, 25, 25, 25]

        self._active_trades[symbol] = {
            "trade_id": trade_id,
            "side": side,
            "initial_qty": qty,
            "remaining_qty": qty,
            "close_pct": close_pct,
            "entry_price": ref_price,
            "sl": payload.get("sl"),
        }

        mode_tag = "DRY-RUN" if Config.DRY_RUN else ("TESTNET" if Config.BYBIT_TESTNET else "CANLI")
        msg = (
            f"🚀 <b>PSL AI YENİ İŞLEM [{mode_tag}]</b>\n"
            f"• Parite: <code>{symbol}</code> ({side.upper()} {leverage}x)\n"
            f"• Miktar: <code>{qty}</code> (~{notional:.2f} USDT hacim)\n"
            f"• Sinyal Fiyatı: <code>{signal_price}</code> | İndikatör SL: <code>{payload.get('sl')}</code>\n"
            f"• Hedefler: TP1={payload.get('tp1')} | TP2={payload.get('tp2')} | TP3={payload.get('tp3')} | TP4={payload.get('tp4')}\n"
            f"• Kaynak: {source} | {latency_info} | Order: <code>{order.get('id')}</code>"
        )
        logger.info(
            "ENTRY çalıştırıldı | %s %s %sx | qty=%s | %s | order=%s",
            symbol,
            side,
            leverage,
            qty,
            latency_info,
            order.get("id"),
        )
        send_telegram(msg)

    def _handle_tp(
        self,
        payload: dict[str, Any],
        event: str,
        symbol: str,
        side: str,
        trade_id: Any,
        source: str,
        latency_info: str,
    ) -> None:
        tp_index = int(event[-1]) - 1
        open_qty = self._get_open_position(symbol, side)
        if open_qty <= 0:
            logger.info("%s için açık pozisyon yok, %s atlandı.", symbol, event.upper())
            return

        trade_state = self._active_trades.get(symbol, {})
        pct = payload.get("close_pct")
        if pct is None:
            pct_list = trade_state.get("close_pct", [25, 25, 25, 25])
            pct = pct_list[tp_index] if tp_index < len(pct_list) else 0
        pct = float(pct)

        new_sl = payload.get("sl")
        if pct <= 0 and event != "tp4":
            # Kâr stopu (trailing SL) modu: pozisyon kapatılmaz, sadece yeni stop seviyesi kaydedilir
            if symbol in self._active_trades:
                self._active_trades[symbol]["sl"] = new_sl
            msg = (
                f"🎯 <b>PSL AI {event.upper()} (Kâr Stobu Güncellendi)</b>\n"
                f"• Parite: <code>{symbol}</code> ({side.upper()})\n"
                f"• Kapatılan: %0 (Kâr Stobu Modu)\n"
                f"• Yeni SL Seviyesi: <code>{new_sl}</code>\n"
                f"• Kaynak: {source} | {latency_info}"
            )
            logger.info("%s %s tetiklendi (%0 kapatma), yeni SL=%s | %s", symbol, event.upper(), new_sl, latency_info)
            send_telegram(msg)
            return

        if event == "tp4" or pct >= 100:
            close_qty = open_qty
        else:
            initial_qty = float(trade_state.get("initial_qty") or open_qty)
            desired_qty = initial_qty * (pct / 100.0)
            close_qty = self._format_amount(symbol, min(desired_qty, open_qty))
            if close_qty <= 0:
                close_qty = open_qty

        close_order_side = "sell" if side == "long" else "buy"
        order = self._place_market_order(
            symbol=symbol,
            order_side=close_order_side,
            amount=close_qty,
            position_side=side,
            reduce_only=True,
        )

        if symbol in self._active_trades:
            remaining = max(0.0, open_qty - close_qty)
            self._active_trades[symbol]["remaining_qty"] = remaining
            self._active_trades[symbol]["sl"] = new_sl
            if event == "tp4" or remaining <= 0:
                self._active_trades.pop(symbol, None)

        msg = (
            f"💰 <b>PSL AI {event.upper()} KÂR ALINDI</b>\n"
            f"• Parite: <code>{symbol}</code> ({side.upper()})\n"
            f"• Kapatılan Miktar: <code>{close_qty}</code> (%{pct:.0f})\n"
            f"• Güncel SL: <code>{new_sl}</code>\n"
            f"• Kaynak: {source} | {latency_info} | Order: <code>{order.get('id')}</code>"
        )
        logger.info("%s %s kapatıldı | qty=%s | %s", symbol, event.upper(), close_qty, latency_info)
        send_telegram(msg)

    def _handle_close(
        self,
        payload: dict[str, Any],
        event: str,
        symbol: str,
        side: str,
        trade_id: Any,
        source: str,
        latency_info: str,
    ) -> None:
        open_qty = self._get_open_position(symbol, side)
        if open_qty <= 0:
            logger.info("%s için açık pozisyon yok, %s atlandı.", symbol, event.upper())
            self._active_trades.pop(symbol, None)
            return

        close_order_side = "sell" if side == "long" else "buy"
        order = self._place_market_order(
            symbol=symbol,
            order_side=close_order_side,
            amount=open_qty,
            position_side=side,
            reduce_only=True,
        )
        self._active_trades.pop(symbol, None)

        title = "🛑 PSL AI STOP LOSS (Mum Kapanışı)" if event == "stop" else "⚠️ PSL AI LİKİDASYON"
        msg = (
            f"{title}\n"
            f"• Parite: <code>{symbol}</code> ({side.upper()})\n"
            f"• Kapatılan Tam Pozisyon: <code>{open_qty}</code>\n"
            f"• Sonuç/PnL (İndikatör): <code>{payload.get('result')}</code> / <code>{payload.get('pnl')}</code>\n"
            f"• Kaynak: {source} | {latency_info} | Order: <code>{order.get('id')}</code>"
        )
        logger.info("%s %s kapatıldı | qty=%s | %s", symbol, event.upper(), open_qty, latency_info)
        send_telegram(msg)


# ---------------------------------------------------------------------------
# Ortak Sinyal Yönlendiricisi
# ---------------------------------------------------------------------------
deduplicator = SignalDeduplicator()
trader: Optional[BybitTrader] = None


def process_signal(
    payload: dict[str, Any],
    source: str,
    email_sent_ts: Optional[float] = None,
) -> dict[str, str]:
    """Webhook veya E-posta üzerinden gelen JSON sinyalini doğrular ve yürütür."""
    if Config.PSL_SECRET_KEY:
        incoming_key = str(payload.get("key", ""))
        if incoming_key != Config.PSL_SECRET_KEY:
            logger.warning("Geçersiz secret key ile sinyal reddedildi (kaynak=%s).", source)
            return {"status": "rejected", "reason": "invalid_key"}

    if deduplicator.is_duplicate(payload):
        logger.debug("Tekrarlayan sinyal atlandı: %s %s", payload.get("symbol"), payload.get("event"))
        return {"status": "ignored", "reason": "duplicate"}

    now = time.time()
    latency_parts: list[str] = []
    if email_sent_ts:
        mail_delay = max(0.0, now - email_sent_ts)
        latency_parts.append(f"Mail Gecikmesi: {mail_delay:.1f}sn")

    sig_time_ms = payload.get("time")
    if isinstance(sig_time_ms, (int, float)) and sig_time_ms > 0:
        diff = now - (float(sig_time_ms) / 1000.0)
        if 0 <= diff < 600:
            latency_parts.append(f"Toplam Sinyal Gecikmesi: {diff:.1f}sn")

    latency_info = " | ".join(latency_parts) if latency_parts else "Anlık"

    if trader is None:
        raise RuntimeError("BybitTrader henüz başlatılmadı.")

    trader.execute_signal(payload, source=source, latency_info=latency_info)
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Gmail IMAP IDLE (Anlık Push E-posta Dinleyici)
# ---------------------------------------------------------------------------
def _parse_email_timestamp(msg: email.message.Message) -> Optional[float]:
    date_header = msg.get("Date")
    if not date_header:
        return None
    try:
        dt = parsedate_to_datetime(date_header)
        return dt.timestamp()
    except Exception:
        return None


def _process_unseen_emails(client: IMAPClient, startup_cutoff_ts: float) -> None:
    uids = client.search(["UNSEEN"])
    if not uids:
        return

    fetched = client.fetch(uids, ["RFC822"])
    for uid, data in fetched.items():
        raw_bytes = data.get(b"RFC822")
        if not raw_bytes:
            continue

        msg = email.message_from_bytes(raw_bytes)
        sender = decode_mime_header(msg.get("From")).lower()
        if Config.ALLOWED_EMAIL_SENDER and Config.ALLOWED_EMAIL_SENDER not in sender:
            continue

        email_ts = _parse_email_timestamp(msg)
        now = time.time()

        # Bot ilk açıldığında gelen kutusundaki eski okunmamış mailleri çalıştırma
        if email_ts is not None:
            if email_ts < startup_cutoff_ts - Config.MAX_EMAIL_AGE_SEC:
                logger.info("Başlangıç öncesi eski okunmamış mail atlandı (UID=%s).", uid)
                client.add_flags([uid], [b"\\Seen"])
                continue
            if (now - email_ts) > Config.MAX_EMAIL_AGE_SEC:
                logger.warning("Süresi geçmiş eski sinyal maili atlandı (%.0fs eski, UID=%s).", now - email_ts, uid)
                client.add_flags([uid], [b"\\Seen"])
                continue

        body_text = extract_email_text(msg)
        payload = extract_psl_json(body_text)
        if not payload:
            # PSL AI sinyali içermeyen normal mailleri okunmamış bırakabiliriz;
            # sadece TradingView'den geliyorsa loglayalım
            if "tradingview" in sender:
                logger.warning("TradingView mailinde JSON formatı bulunamadı (UID=%s). İndikatörde Format=JSON seçili mi?", uid)
            continue

        client.add_flags([uid], [b"\\Seen"])
        try:
            process_signal(payload, source="Gmail IMAP IDLE", email_sent_ts=email_ts)
        except Exception as exc:
            logger.exception("E-posta sinyali işlenirken hata oluştu (UID=%s): %s", uid, exc)
            send_telegram(f"❌ <b>E-posta Sinyali Hatası:</b> <code>{exc}</code>")


def imap_idle_worker(stop_event: threading.Event) -> None:
    """Gmail sunucusuna kalıcı soket açıp yeni e-postaları saniyeler içinde yakalar."""
    if not Config.GMAIL_ADDRESS or not Config.GMAIL_APP_PASSWORD:
        logger.warning(
            "GMAIL_ADDRESS veya GMAIL_APP_PASSWORD eksik! IMAP IDLE dinleyici başlatılmadı."
        )
        return

    startup_cutoff_ts = time.time()
    logger.info("Gmail IMAP IDLE dinleyici başlatılıyor (%s)...", Config.GMAIL_ADDRESS)

    while not stop_event.is_set():
        client: Optional[IMAPClient] = None
        try:
            client = IMAPClient(Config.IMAP_HOST, port=Config.IMAP_PORT, ssl=True, timeout=30)
            client.login(Config.GMAIL_ADDRESS, Config.GMAIL_APP_PASSWORD)
            client.select_folder(Config.IMAP_FOLDER)
            logger.info("Gmail IMAP IDLE bağlantısı kuruldu. Anlık e-posta bekleniyor...")

            # Bağlanır bağlanmaz yeni okunmamış varsa tara
            _process_unseen_emails(client, startup_cutoff_ts)

            while not stop_event.is_set():
                client.idle()
                # Gmail IDLE oturumunu koparmamak için her 240 saniyede bir yenile
                responses = client.idle_check(timeout=240)
                client.idle_done()
                if responses:
                    _process_unseen_emails(client, startup_cutoff_ts)
        except Exception as exc:
            logger.error("IMAP IDLE bağlantı hatası (5 sn sonra yeniden bağlanılacak): %s", exc)
            stop_event.wait(5)
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# FastAPI Yaşam Döngüsü, Türkçe Web Paneli ve Endpointler
# ---------------------------------------------------------------------------
_stop_event = threading.Event()
_imap_thread: Optional[threading.Thread] = None
tv_watcher: Optional[TradingViewPlaywrightWatcher] = None


def _bot_mode_label() -> str:
    if Config.DRY_RUN:
        return "DRY-RUN (Sadece Simülasyon / Emir Yok)"
    if trader and trader.connected:
        return trader.account_mode_name
    if Config.BYBIT_TESTNET:
        return "TESTNET / DEMO (Sanal Para)"
    return "CANLI (Gerçek Hesap)"


def _render_coin_card(coin_data: dict[str, Any], esc: Any) -> str:
    parsed = coin_data.get("parsed_table") or {}
    active = coin_data.get("active_trade")
    active_html = "Yok (Yeni sinyal bekleniyor)"
    if active:
        active_html = (
            f"{esc(active.get('side', '')).upper()} | Giriş: {esc(active.get('entry'))} | "
            f"SL: {esc(active.get('sl'))} | Vurulan TP: {esc(', '.join(active.get('hit_tps') or []) or 'Yok')}"
        )

    return f"""
      <div class="card">
        <h2>📊 {esc(coin_data.get('symbol'))} · {esc(coin_data.get('interval'))}</h2>
        <div class="row"><span class="lbl">Anlık Fiyat (Bybit)</span><span class="val">{esc(coin_data.get('live_price'))}</span></div>
        <div class="row"><span class="lbl">İndikatör Algılandı</span><span class="val">{'✅ Evet' if coin_data.get('indicator_detected') else '⏳ Bekleniyor'}</span></div>
        <div class="row"><span class="lbl">Sonuç</span><span class="val">{esc(parsed.get('verdict'))}</span></div>
        <div class="row"><span class="lbl">Plan Yönü / Durumu</span><span class="val">{esc(parsed.get('plan_side')).upper()} · {esc(parsed.get('plan_status_text') or parsed.get('plan_status'))}</span></div>
        <div class="row"><span class="lbl">GİRİŞ</span><span class="val">{esc(parsed.get('entry'))}</span></div>
        <div class="row"><span class="lbl">HEDEF 1 / 2</span><span class="val">{esc(parsed.get('tp1'))} / {esc(parsed.get('tp2'))}</span></div>
        <div class="row"><span class="lbl">HEDEF 3 / 4</span><span class="val">{esc(parsed.get('tp3'))} / {esc(parsed.get('tp4'))}</span></div>
        <div class="row"><span class="lbl">GEÇERSİZ (SL)</span><span class="val">{esc(parsed.get('sl'))}</span></div>
        <div class="row"><span class="lbl">Bot Aktif İşlem</span><span class="val">{active_html}</span></div>
        <div class="row"><span class="lbl">Son Güncelleme</span><span class="val">{esc(coin_data.get('last_update'))}</span></div>
      </div>"""


def _render_dashboard_html() -> str:
    mode_label = _bot_mode_label()
    status = tv_watcher.get_status() if tv_watcher else {}

    bybit_conn_text = "❌ Bağlı Değil"
    balance_text = "-"
    if Config.DRY_RUN:
        bybit_conn_text = "🧪 Simülasyon (DRY-RUN)"
    elif trader and trader.connected:
        bybit_conn_text = f"✅ Bağlı ({trader.account_mode_name})"
        bal = trader.refresh_balance()
        if bal is not None:
            balance_text = f"{bal:.2f} USDT"

    esc = lambda v: html.escape(str(v)) if v not in (None, "") else "-"
    login_banner = ""
    if status.get("needs_login_hint"):
        login_banner = (
            "<div class='warn'>"
            "⚠️ TradingView grafikte PSL AI tablosu henüz okunamadı veya oturum çerezi gerekiyor. "
            "Hugging Face Secrets kısmına <b>TV_SESSIONID</b> eklerseniz kendi kayıtlı grafiğiniz yüklenir."
            "</div>"
        )

    tf_lev_text = (
        "Aktif (5dk:50x, 15dk:20x, 1s:15x, 4s:8x)"
        if Config.AUTO_TF_LEVERAGE
        else f"Kapalı (Sabit {Config.DEFAULT_LEVERAGE}x)"
    )

    coins_dict = status.get("coins") or {}
    if coins_dict:
        coin_cards_html = "".join(_render_coin_card(c, esc) for c in coins_dict.values())
    else:
        fallback_coin = {
            "symbol": status.get("symbol") or Config.TV_SYMBOL,
            "interval": status.get("interval") or Config.TV_INTERVAL,
            "live_price": status.get("live_price"),
            "indicator_detected": status.get("indicator_detected"),
            "parsed_table": status.get("parsed_table") or {},
            "active_trade": status.get("active_trade"),
            "last_update": status.get("last_update"),
        }
        coin_cards_html = _render_coin_card(fallback_coin, esc)

    return f"""<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PSL AI v2.1 — Bybit Bulut Botu</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0d1117; color: #e6edf3; margin: 0; padding: 20px; }}
    .wrap {{ max-width: 1120px; margin: 0 auto; }}
    h1 {{ font-size: 22px; margin-bottom: 6px; }}
    .sub {{ color: #8b949e; font-size: 14px; margin-bottom: 18px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px; margin-bottom: 18px; }}
    .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 14px 16px; }}
    .card h2 {{ font-size: 15px; margin: 0 0 10px 0; color: #58a6ff; }}
    .row {{ display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px solid #21262d; font-size: 14px; }}
    .row:last-child {{ border-bottom: none; }}
    .lbl {{ color: #8b949e; }}
    .val {{ font-weight: 600; text-align: right; }}
    .warn {{ background: #3b2608; border: 1px solid #9e6a03; color: #f0b429; padding: 12px 14px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; }}
    .shot-box {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 14px; }}
    .shot-box img {{ width: 100%; border-radius: 6px; border: 1px solid #30363d; display: block; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>🤖 PSL AI v2.1 — Bybit Bulut Bot Paneli</h1>
    <div class="sub">Sayfa 10 saniyede bir otomatik yenilenir · Canlı Durum</div>
    {login_banner}
    <div class="grid">
      <div class="card">
        <h2>⚙️ Bot ve Bybit Ayarları</h2>
        <div class="row"><span class="lbl">Bybit Bağlantısı</span><span class="val">{esc(bybit_conn_text)}</span></div>
        <div class="row"><span class="lbl">Cüzdan Bakiyesi</span><span class="val">{esc(balance_text)}</span></div>
        <div class="row"><span class="lbl">Çalışma Modu</span><span class="val">{esc(mode_label)}</span></div>
        <div class="row"><span class="lbl">İşlem Başı Marjin</span><span class="val">{Config.MARGIN_USDT} USDT</span></div>
        <div class="row"><span class="lbl">Maks. Kaldıraç</span><span class="val">{Config.MAX_LEVERAGE}x</span></div>
        <div class="row"><span class="lbl">Otomatik Zaman Dilimi Kaldıracı</span><span class="val">{esc(tf_lev_text)}</span></div>
        <div class="row"><span class="lbl">Bulut Tarayıcı</span><span class="val">{'Aktif' if Config.ENABLE_PLAYWRIGHT else 'Kapalı'} ({esc(status.get('state'))})</span></div>
        <div class="row"><span class="lbl">Mail (IMAP)</span><span class="val">{'Aktif' if Config.ENABLE_IMAP else 'Kapalı'}</span></div>
      </div>
      {coin_cards_html}
    </div>

    <div class="shot-box">
      <h2>🖥️ Bulut Tarayıcı Canlı Ekranı</h2>
      <img id="tvShot" src="/screenshot?t={int(time.time())}" alt="TradingView Canlı Ekran Görüntüsü (Henüz hazırlanıyor...)" />
    </div>
  </div>

  <script>
    setInterval(() => {{
      window.location.reload();
    }}, 10000);
  </script>
</body>
</html>"""


@asynccontextmanager
async def lifespan(_: FastAPI):
    global trader, _imap_thread, tv_watcher
    trader = BybitTrader()

    if Config.ENABLE_PLAYWRIGHT:
        tv_watcher = TradingViewPlaywrightWatcher(
            tv_symbol=Config.TV_SYMBOL,
            tv_interval=Config.TV_INTERVAL,
            signal_callback=process_signal,
            chart_url=Config.TV_CHART_URL,
            sessionid=Config.TV_SESSIONID,
            sessionid_sign=Config.TV_SESSIONID_SIGN,
            poll_sec=Config.TV_POLL_SEC,
            secret_key=Config.PSL_SECRET_KEY,
            default_leverage=Config.DEFAULT_LEVERAGE,
            bybit_testnet=(trader.account_mode_name == "TESTNET"),
        )
        tv_watcher.start()

    if Config.ENABLE_IMAP:
        _imap_thread = threading.Thread(
            target=imap_idle_worker,
            args=(_stop_event,),
            daemon=True,
            name="imap-idle-worker",
        )
        _imap_thread.start()

    mode = _bot_mode_label()
    bal_str = f"{trader.usdt_balance:.2f} USDT" if trader.usdt_balance is not None else "-"
    send_telegram(
        f"✅ <b>PSL AI Bybit Bot Başlatıldı</b>\n"
        f"• Mod: <b>{mode}</b> (Bakiye: <code>{bal_str}</code>)\n"
        f"• Parite: <code>{Config.TV_SYMBOL}</code> ({Config.TV_INTERVAL})\n"
        f"• İşlem Başı Marjin: <code>{Config.MARGIN_USDT} USDT</code> | Maks Kaldıraç: <code>{Config.MAX_LEVERAGE}x</code>\n"
        f"• Bulut Tarayıcı (Playwright): <b>{'Aktif' if Config.ENABLE_PLAYWRIGHT else 'Kapalı'}</b>\n"
        f"• E-posta Dinleyici: <b>{'Aktif' if Config.ENABLE_IMAP else 'Kapalı'}</b>"
    )
    yield
    _stop_event.set()
    if tv_watcher:
        tv_watcher.stop()
    if _imap_thread and _imap_thread.is_alive():
        _imap_thread.join(timeout=3)


app = FastAPI(title="PSL AI v2.1 Bybit Bot", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    """Kullanıcının buluttaki botu ve TradingView ekranını izleyebileceği Türkçe panel."""
    return _render_dashboard_html()


@app.get("/health")
def health_check() -> dict[str, Any]:
    """UptimeRobot gibi servislerin botu uyanık tutması için sağlık kontrolü."""
    return {
        "status": "ok",
        "mode": _bot_mode_label(),
        "bybit_connected": bool(trader and trader.connected),
        "usdt_balance": trader.usdt_balance if trader else None,
        "playwright_enabled": Config.ENABLE_PLAYWRIGHT,
        "playwright_status": tv_watcher.get_status() if tv_watcher else None,
        "imap_enabled": Config.ENABLE_IMAP,
        "imap_alive": bool(_imap_thread and _imap_thread.is_alive()),
    }


@app.get("/tv-status")
def tv_status() -> dict[str, Any]:
    """TradingView bulut tarayıcısından okunan güncel tablo verilerini JSON olarak döndürür."""
    if not tv_watcher:
        return {"enabled": False}
    return tv_watcher.get_status()


@app.get("/screenshot")
def tv_screenshot() -> Response:
    """Bulut tarayıcıda açık olan TradingView grafiğinin son ekran görüntüsünü döndürür."""
    if not tv_watcher:
        raise HTTPException(status_code=404, detail="Playwright tarayıcı kapalı.")
    png = tv_watcher.get_screenshot()
    if not png:
        raise HTTPException(status_code=503, detail="Ekran görüntüsü henüz hazırlanıyor.")
    return Response(content=png, media_type="image/png")


@app.post("/webhook")
async def tradingview_webhook(request: Request) -> dict[str, str]:
    """Doğrudan TradingView Webhook veya Email-to-Webhook köprüsü için HTTP endpoint."""
    raw_body = (await request.body()).decode("utf-8", errors="replace")
    payload = extract_psl_json(raw_body)
    if not payload:
        raise HTTPException(status_code=400, detail="Geçerli PSL AI JSON verisi bulunamadı.")

    result = process_signal(payload, source="HTTP Webhook")
    if result.get("status") == "rejected":
        raise HTTPException(status_code=403, detail="Geçersiz secret key.")
    return result


if __name__ == "__main__":
    uvicorn.run("bot:app", host=Config.HOST, port=Config.PORT, log_level="info")
