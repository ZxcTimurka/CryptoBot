import os
import re
import csv
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from dotenv import load_dotenv
from telethon import TelegramClient, events
from pybit.unified_trading import HTTP

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG
)
logging.getLogger("telethon").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- 2. Загрузка конфигурации ---
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

# Доп.настройки: список спотовых инструментов (через .env в виде "BTCUSDT,ETHUSDT,XRPUSDT")
SPOT_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("SPOT_SYMBOLS", "BTCUSDT,ETHUSDT,XRPUSDT,ADAUSDT").split(",")
    if s.strip()
]

# TRADE_AMOUNT_USD фиксированно на каждую сделку
TRADE_AMOUNT_USD = Decimal(os.getenv("TRADE_AMOUNT_USD", "1000"))

# Настройка точностей: можешь задать в .env в формате "BNBUSDT:6,BTCUSDT:6,ETHUSDT:6"
# По умолчанию — DEFAULT_BASE_PRECISION (если инструмент не указан в карте)
DEFAULT_BASE_PRECISION = int(os.getenv("DEFAULT_BASE_PRECISION", "3"))
_spot_decimals_env = os.getenv("SPOT_DECIMALS", "")
SPOT_DECIMALS = {}
if _spot_decimals_env:
    for pair in _spot_decimals_env.split(","):
        if ":" in pair:
            k, v = pair.split(":", 1)
            try:
                SPOT_DECIMALS[k.strip().upper()] = int(v.strip())
            except Exception:
                continue

# --- Файлы для логов/хранения позиций ---
TRADES_CSV = "trades.csv"
POSITIONS_JSON = "positions.json"

if not os.path.exists(TRADES_CSV):
    with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp",
                "symbol",
                "side",
                "order_id",
                "exec_price",
                "exec_qty",
                "fee",
                "fee_currency",
                "realized_pnl",
                "notes",
            ]
        )


# --- Вспомогательные функции ---
def load_positions():
    if os.path.exists(POSITIONS_JSON):
        with open(POSITIONS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_positions(positions):
    with open(POSITIONS_JSON, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)


def write_trade_row(row):
    # timezone-aware timestamp must be passed in the row already
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def to_decimal(x):
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(0)


def round_down_decimal(value: Decimal, precision: int) -> Decimal:
    """
    Округляет Decimal value вниз до precision знаков после запятой.
    precision = 0 -> целое число.
    """
    try:
        value = to_decimal(value)
        if precision <= 0:
            quant = Decimal("1")
        else:
            quant_str = "0." + ("0" * (precision - 1)) + "1"
            quant = Decimal(quant_str)
        return value.quantize(quant, rounding=ROUND_DOWN)
    except Exception:
        # на случай ошибок — возвращаем исходное, но не None
        return value


# FIFO matching: добавляем позиции при BUY, закрываем при SELL
def update_positions_and_compute_pnl(symbol, side, price, qty, fee=0):
    positions = load_positions()
    pos = positions.get(symbol, {"longs": [], "shorts": [], "realized_pnl_total": "0"})
    realized = Decimal(pos.get("realized_pnl_total", "0"))
    price = to_decimal(price)
    qty = to_decimal(qty)
    fee = to_decimal(fee)

    if side.lower() == "buy":
        shorts = pos.get("shorts", [])
        remaining = qty
        if shorts:
            new_shorts = []
            for s in shorts:
                s_qty = to_decimal(s["qty"])
                s_price = to_decimal(s["price"])
                if remaining <= 0:
                    new_shorts.append(s)
                    continue
                close_qty = min(s_qty, remaining)
                pnl = (s_price - price) * close_qty
                realized += pnl - fee
                remaining -= close_qty
                if s_qty > close_qty:
                    new_shorts.append(
                        {"qty": str(s_qty - close_qty), "price": str(s_price)}
                    )
            pos["shorts"] = new_shorts
            if remaining > 0:
                pos.setdefault("longs", []).append(
                    {"qty": str(remaining), "price": str(price)}
                )
        else:
            pos.setdefault("longs", []).append({"qty": str(qty), "price": str(price)})
    else:  # Sell
        longs = pos.get("longs", [])
        remaining = qty
        if longs:
            new_longs = []
            for l in longs:
                l_qty = to_decimal(l["qty"])
                l_price = to_decimal(l["price"])
                if remaining <= 0:
                    new_longs.append(l)
                    continue
                close_qty = min(l_qty, remaining)
                pnl = (price - l_price) * close_qty
                realized += pnl - fee
                remaining -= close_qty
                if l_qty > close_qty:
                    new_longs.append(
                        {"qty": str(l_qty - close_qty), "price": str(l_price)}
                    )
            pos["longs"] = new_longs
            if remaining > 0:
                pos.setdefault("shorts", []).append(
                    {"qty": str(remaining), "price": str(price)}
                )
        else:
            pos.setdefault("shorts", []).append({"qty": str(qty), "price": str(price)})

    pos["realized_pnl_total"] = str(realized)
    positions[symbol] = pos
    save_positions(positions)
    return realized


# --- 3. Инициализация клиентов ---
try:
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    logger.info("Клиент Telethon инициализирован.")

    session = HTTP(
        testnet=False,
        demo=True,
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
    )
    logger.info("Клиент Bybit (pybit HTTP) инициализирован для Testnet.")
except Exception as e:
    logger.error(f"Ошибка при инициализации клиентов: {e}")
    raise SystemExit(1)


# --- 4. Парсер сигналов ---
def parse_signal(message):
    long_pattern = re.compile(
        r"(?:⭐️ 'TOP 5'.*)?🚀\s*(?P<symbol>.*?/USDT)\s+LONG on BINANCE.*✅\s*BUYING COMPLETED.*📈\s*AVERAGE PRICE:\s*(?P<price>[\d.,]+)\s+USDT",
        re.DOTALL | re.IGNORECASE,
    )

    close_pattern = re.compile(
        r"(?:⭐️ 'TOP 5'.*)?❌\s*(?P<symbol>.*?/USDT)\s+on BINANCE.*🆑\s*POSITION CLOSED.*📉\s*AVERAGE PRICE:\s*(?P<price>[\d.,]+)\s+USDT",
        re.DOTALL | re.IGNORECASE,
    )

    match_long = long_pattern.search(message)
    if match_long:
        data = match_long.groupdict()
        data["side"] = "Buy"
        data["symbol"] = data["symbol"].replace("/", "").upper()
        logger.info(f"Распознан LONG сигнал: {data}")
        return data

    match_close = close_pattern.search(message)
    if match_close:
        data = match_close.groupdict()
        data["side"] = "Sell"
        data["symbol"] = data["symbol"].replace("/", "").upper()
        logger.info(f"Распознан CLOSE сигнал: {data}")
        return data

    return None


# --- Вспомогательная: извлечь fills из response ---
def extract_fills_from_response(response):
    fills = []
    if not response:
        return fills

    def find_execs(obj):
        found = []
        if isinstance(obj, list):
            for el in obj:
                found += find_execs(el)
        elif isinstance(obj, dict):
            keys = set(k.lower() for k in obj.keys())
            if (
                "price" in keys
                or "execprice" in keys
                or "avgprice" in keys
                or "fillednotional" in keys
            ) and (
                "qty" in keys
                or "execqty" in keys
                or "filledqty" in keys
                or "orderqty" in keys
            ):
                found.append(obj)
            else:
                for v in obj.values():
                    found += find_execs(v)
        return found

    candidates = []
    if isinstance(response, dict):
        for key in ("result", "data", "response"):
            if key in response:
                candidates.append(response[key])
        candidates.append(response)

    for c in candidates:
        found = find_execs(c)
        for f in found:
            price = None
            qty = None
            fee = Decimal("0")
            order_id = None
            for k, v in f.items():
                kl = k.lower()
                if (
                    "price" in kl or "execprice" in kl or "avgprice" in kl
                ) and price is None:
                    try:
                        price = Decimal(str(v))
                    except Exception:
                        pass
                if (
                    "qty" in kl
                    or "filledqty" in kl
                    or "execqty" in kl
                    or "orderqty" in kl
                    or "fillednotional" in kl
                ) and qty is None:
                    try:
                        qty = Decimal(str(v))
                    except Exception:
                        pass
                if "fee" in kl or "tradingfee" in kl:
                    try:
                        fee = Decimal(str(v))
                    except Exception:
                        pass
                if "order" in kl and order_id is None:
                    order_id = str(v)
            if price is not None and qty is not None:
                fills.append(
                    {"price": price, "qty": qty, "fee": fee, "order_id": order_id}
                )
    return fills


# --- New helper: получить баланс USDT (обновлённый, см. предыдущие сообщения) ---
def get_usdt_balance():
    try:
        resp = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        logger.debug("get_wallet_balance(UNIFIED) response: %s", resp)

        result = resp.get("result", resp)

        if isinstance(result, dict):
            tavail = result.get("totalAvailableBalance")
            if tavail is not None:
                try:
                    return to_decimal(tavail)
                except Exception:
                    pass

            lst = result.get("list")
            if isinstance(lst, list) and len(lst) > 0:
                first = lst[0]
                if isinstance(first, dict):
                    tav = (
                        first.get("totalAvailableBalance")
                        or first.get("totalWalletBalance")
                        or first.get("totalMarginBalance")
                    )
                    if tav is not None:
                        try:
                            return to_decimal(tav)
                        except Exception:
                            pass

                    coins = first.get("coin")
                    if isinstance(coins, list):
                        for sub in coins:
                            if not isinstance(sub, dict):
                                continue
                            c = (sub.get("coin") or sub.get("currency") or "").upper()
                            if c == "USDT":
                                for k in (
                                    "availableBalance",
                                    "walletBalance",
                                    "usdValue",
                                    "equity",
                                    "availableToWithdraw",
                                    "balance",
                                    "totalBalance",
                                ):
                                    if k in sub:
                                        try:
                                            return to_decimal(sub.get(k))
                                        except Exception:
                                            continue

        def recursive_search_for_usdt(obj):
            if isinstance(obj, dict):
                coin_val = None
                if "coin" in obj and isinstance(obj["coin"], str):
                    coin_val = obj["coin"].upper()
                if "currency" in obj and isinstance(obj["currency"], str):
                    coin_val = obj["currency"].upper()
                if coin_val == "USDT":
                    for k in (
                        "availableBalance",
                        "available",
                        "walletBalance",
                        "usdValue",
                        "totalAvailableBalance",
                        "totalWalletBalance",
                        "balance",
                        "totalBalance",
                        "equity",
                    ):
                        if k in obj:
                            try:
                                return to_decimal(obj.get(k))
                            except Exception:
                                continue
                for v in obj.values():
                    res = recursive_search_for_usdt(v)
                    if res is not None and res != Decimal("0"):
                        return res
            elif isinstance(obj, list):
                for el in obj:
                    res = recursive_search_for_usdt(el)
                    if res is not None and res != Decimal("0"):
                        return res
            return None

        rec = recursive_search_for_usdt(resp)
        if rec is not None:
            return rec

        logger.debug(
            "get_usdt_balance: не удалось найти явный USDT в result structure. Full resp logged above."
        )
    except Exception as e:
        logger.warning(f"Не удалось получить баланс UNIFIED: {e}")

    logger.info("Баланс USDT не найден — вернул 0.")
    return Decimal("0")


# --- NEW helpers: qty precision / local base qty ---
def get_local_long_qty(symbol):
    positions = load_positions()
    pos = positions.get(symbol, {})
    longs = pos.get("longs", [])
    total = Decimal("0")
    for l in longs:
        try:
            total += to_decimal(l.get("qty", "0"))
        except Exception:
            continue
    return total


def get_base_balance_from_api(symbol):
    base = symbol.replace("USDT", "")
    try:
        resp = session.get_wallet_balance(accountType="SPOT", coin=base)
        lst = resp.get("result", {}).get("list", [])
        for item in lst:
            coin = (item.get("coin") or item.get("currency") or "").upper()
            if coin == base:
                for k in (
                    "availableBalance",
                    "available",
                    "totalAvailableBalance",
                    "balance",
                    "totalBalance",
                    "walletBalance",
                ):
                    if k in item:
                        return to_decimal(item.get(k))
    except Exception as e:
        logger.warning(f"Не удалось получить баланс {base} через SPOT API: {e}")
    return Decimal("0")


def get_precision_for_symbol(symbol: str) -> int:
    """
    Возвращает количество знаков после запятой для базовой валюты символа, если указано в SPOT_DECIMALS.
    Иначе — DEFAULT_BASE_PRECISION.
    """
    return SPOT_DECIMALS.get(symbol.upper(), DEFAULT_BASE_PRECISION)


# --- NEW: close_spot_position with precision-aware rounding ---
def close_spot_position(symbol):
    try:
        base_qty = get_local_long_qty(symbol)
        if base_qty <= 0:
            base_qty = get_base_balance_from_api(symbol)

        if base_qty <= 0:
            logger.info(
                f"Нет лонгов для {symbol} (локально и в API) — нечего закрывать."
            )
            write_trade_row(
                [
                    datetime.now(timezone.utc).isoformat(),
                    symbol,
                    "Sell",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "nothing_to_close",
                ]
            )
            return

        precision = get_precision_for_symbol(symbol)
        base_qty = round_down_decimal(base_qty, precision)
        if base_qty <= 0:
            logger.info(
                f"После округления qty={base_qty} — ничтожно, не отправляю ордер."
            )
            return

        logger.info(
            f"Попытка закрыть спот-лонг: {symbol}, qty={base_qty} (market sell) with precision={precision}"
        )

        response = session.place_order(
            category="spot",
            symbol=symbol,
            side="Sell",
            orderType="Market",
            qty=str(base_qty),
        )

        logger.debug(f"Ответ Bybit на close (raw): {response}")

        fills = extract_fills_from_response(response)
        if not fills:
            price_decimal = None
            try:
                ticker = session.get_tickers(category="spot", symbol=symbol)
                tlist = ticker.get("result", {}).get("list", [])
                if tlist:
                    ask = (
                        tlist[0].get("bid1Price")
                        or tlist[0].get("lastPrice")
                        or tlist[0].get("price")
                    )
                    if ask:
                        price_decimal = Decimal(str(ask))
            except Exception:
                price_decimal = None

            fills = [
                {
                    "price": price_decimal
                    if price_decimal is not None
                    else Decimal("0"),
                    "qty": base_qty,
                    "fee": Decimal("0"),
                    "order_id": str(
                        response.get("orderId") if isinstance(response, dict) else ""
                    ),
                }
            ]

        for f in fills:
            price = f["price"]
            qty = f["qty"]
            fee = f.get("fee", Decimal("0"))
            order_id = f.get("order_id", "")
            realized = update_positions_and_compute_pnl(symbol, "Sell", price, qty, fee)
            write_trade_row(
                [
                    datetime.now(timezone.utc).isoformat(),
                    symbol,
                    "Sell",
                    order_id,
                    str(price),
                    str(qty),
                    str(fee),
                    "USDT",
                    str(realized),
                    "closed_by_signal",
                ]
            )
            logger.info(
                f"Closed logged: {symbol} qty={qty} price={price} realized={realized}"
            )

    except Exception as e:
        logger.error(f"Ошибка при попытке закрыть позицию {symbol}: {e}")
        write_trade_row(
            [
                datetime.now(timezone.utc).isoformat(),
                symbol,
                "Sell",
                "",
                "",
                "",
                "",
                "",
                "",
                f"close_error: {e}",
            ]
        )


# --- 5. Функция для размещения ордера (Buy и Sell обработка) ---
def place_order_on_bybit(signal_data):
    try:
        symbol = signal_data["symbol"].upper()
        side = signal_data["side"].capitalize()  # Buy / Sell

        if symbol not in SPOT_SYMBOLS:
            logger.warning(
                f"Сигнал по {symbol} — инструмент не в списке SPOT_SYMBOLS, игнорирую."
            )
            return

        if side == "Buy":
            balance_usdt = get_usdt_balance()
            logger.info(f"Баланс USDT (доступный): {balance_usdt}")

            desired = TRADE_AMOUNT_USD
            if balance_usdt <= 0:
                logger.warning("Баланс USDT нулевой — не размещаю ордер.")
                write_trade_row(
                    [
                        datetime.now(timezone.utc).isoformat(),
                        symbol,
                        "Buy",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "no_balance",
                    ]
                )
                return

            if balance_usdt < desired:
                amount_usdt = (balance_usdt * Decimal("0.99")).quantize(Decimal("0.01"))
                logger.info(
                    f"Баланс меньше {desired}$ — установлено amount_usdt = {amount_usdt}"
                )
            else:
                amount_usdt = desired

            raw_price = signal_data.get("price")
            price_decimal = None
            if raw_price:
                try:
                    price_decimal = Decimal(raw_price.replace(",", "."))
                except Exception:
                    price_decimal = None

            if price_decimal is None:
                try:
                    ticker = session.get_tickers(category="spot", symbol=symbol)
                    tlist = ticker.get("result", {}).get("list", [])
                    if tlist:
                        ask = (
                            tlist[0].get("ask1Price")
                            or tlist[0].get("lastPrice")
                            or tlist[0].get("price")
                        )
                        if ask:
                            price_decimal = Decimal(str(ask))
                    logger.info(f"Получили цену из тикера: {price_decimal}")
                except Exception as e:
                    logger.warning(f"Не удалось получить тикер для {symbol}: {e}")

            logger.info(
                f"Попытка Market Buy (spot) {symbol} за {amount_usdt} USDT (marketUnit=quoteCoin)"
            )

            response = session.place_order(
                category="spot",
                symbol=symbol,
                side="Buy",
                orderType="Market",
                qty=str(amount_usdt),
                marketUnit="quoteCoin",
            )

            logger.debug(f"Ответ Bybit (raw): {response}")

            fills = extract_fills_from_response(response)

            if not fills:
                fallback_price_val = price_decimal
                if fallback_price_val is not None and fallback_price_val != 0:
                    # qty(base) = amount_usdt / price
                    try:
                        raw_base_qty = to_decimal(amount_usdt) / fallback_price_val
                    except Exception:
                        raw_base_qty = Decimal("0")
                    precision = get_precision_for_symbol(symbol)
                    base_qty = round_down_decimal(raw_base_qty, precision)
                    fills = [
                        {
                            "price": fallback_price_val,
                            "qty": base_qty,
                            "fee": Decimal("0"),
                            "order_id": str(
                                response.get("orderId")
                                if isinstance(response, dict)
                                else ""
                            ),
                        }
                    ]
                else:
                    logger.warning(
                        "Не удалось извлечь fills и нет цены для fallback — логирую событие."
                    )
                    write_trade_row(
                        [
                            datetime.now(timezone.utc).isoformat(),
                            symbol,
                            "Buy",
                            str(
                                response.get("orderId")
                                if isinstance(response, dict)
                                else ""
                            ),
                            "",
                            "",
                            "",
                            "",
                            "",
                            "no_fills_found",
                        ]
                    )
                    return

            for f in fills:
                price = f["price"]
                qty = f["qty"]
                fee = f.get("fee", Decimal("0"))
                order_id = f.get("order_id", "")

                realized = update_positions_and_compute_pnl(
                    symbol, "Buy", price, qty, fee
                )
                write_trade_row(
                    [
                        datetime.now(timezone.utc).isoformat(),
                        symbol,
                        "Buy",
                        order_id,
                        str(price),
                        str(qty),
                        str(fee),
                        "USDT",
                        str(realized),
                        "ok",
                    ]
                )

                logger.info(
                    f"Exec logged: symbol={symbol} side=Buy price={price} qty={qty} realized_pnl={realized}"
                )

        elif side == "Sell":
            close_spot_position(symbol)

        else:
            logger.warning(f"Неизвестный side={side} — игнор.")
            return

    except Exception as e:
        logger.error(f"Ошибка при размещении ордера на Bybit: {e}")
        write_trade_row(
            [
                datetime.now(timezone.utc).isoformat(),
                signal_data.get("symbol", ""),
                signal_data.get("side", ""),
                "",
                "",
                "",
                "",
                "",
                "",
                f"place_order_error: {e}",
            ]
        )


# --- 6. Обработчик сообщений ---
try:
    target_channel = int(TELEGRAM_CHANNEL_ID)
except Exception:
    target_channel = TELEGRAM_CHANNEL_ID


@client.on(events.NewMessage(chats=target_channel))
async def handler(event):
    message_text = event.message.text or ""
    logger.info("Получено новое сообщение из канала.")
    signal = parse_signal(message_text)

    if signal:
        # place_order_on_bybit синхронная — можно вызывать прямо
        place_order_on_bybit(signal)
    else:
        logger.warning(
            f"Сообщение не является торговым сигналом или не соответствует шаблону. Результат парсинга (signal): {signal}"
        )


# --- 7. Запуск ---
async def main():
    await client.start()
    logger.info("Бот успешно запущен и ожидает новые сообщения...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
