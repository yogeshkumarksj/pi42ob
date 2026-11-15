"""
Pi42 Live L2 Order Book (OBI) + Trade Stream Collector
------------------------------------------------------
Install dependencies once:
    pip install "python-socketio[client]" websocket-client pandas pyarrow

Run:
    python pi42_obi_collector.py
"""

import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta

import pandas as pd
import socketio

# ================= CONFIGURATION =================
PI42_WS_URL = "https://fawss.pi42.com/"
CONTRACT_PAIR = "ETHINR"      # Change to "ETHUSDT" if needed
SAVE_DIR = "./pi42_obi_data"
FLUSH_INTERVAL_SEC = 60       # Save to disk every 60 seconds
SAMPLE_INTERVAL_SEC = 1.0     # Record at most 1 snapshot per second
# =================================================

IST = timezone(timedelta(hours=5, minutes=30))
os.makedirs(SAVE_DIR, exist_ok=True)

sio = socketio.Client(
    reconnection=True,
    reconnection_attempts=0,   # Infinite reconnect attempts
    reconnection_delay=2,
    reconnection_delay_max=30,
    logger=False,
    engineio_logger=False,
)

buffer_rows = []
recent_trades = deque(maxlen=5000)  # Stores (ts_sec, signed_qty) for 30s CVD
last_sample_ts = 0.0
last_flush_ts = time.time()
running = True


def compute_obi(bids, asks, depth: int) -> tuple[float, float, float]:
    b_slice = bids[:depth]
    a_slice = asks[:depth]
    bid_vol = sum(float(q) for _, q in b_slice)
    ask_vol = sum(float(q) for _, q in a_slice)
    obi = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-9)
    return obi, bid_vol, ask_vol


def flush_to_disk():
    global buffer_rows, last_flush_ts
    if not buffer_rows:
        last_flush_ts = time.time()
        return

    df_new = pd.DataFrame(buffer_rows)
    date_str = datetime.now(IST).strftime("%Y%m%d")
    csv_path = os.path.join(SAVE_DIR, f"{CONTRACT_PAIR}_obi_{date_str}.csv")
    parquet_path = os.path.join(SAVE_DIR, f"{CONTRACT_PAIR}_obi_{date_str}.parquet")

    write_header = not os.path.exists(csv_path)
    df_new.to_csv(csv_path, mode="a", header=write_header, index=False)

    if os.path.exists(parquet_path):
        df_old = pd.read_parquet(parquet_path)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_combined = df_new
    df_combined.to_parquet(parquet_path, index=False)

    print(
        f"[{datetime.now(IST).strftime('%H:%M:%S')}] Flushed {len(df_new)} rows "
        f"(Total today: {len(df_combined):,}) -> {parquet_path}"
    )
    buffer_rows = []
    last_flush_ts = time.time()


@sio.event
def connect():
    print(f"[{datetime.now(IST).strftime('%H:%M:%S')}] Connected to Pi42 WebSocket ({PI42_WS_URL})")
    streams = [f"{CONTRACT_PAIR}@depth20", f"{CONTRACT_PAIR}@aggTrade"]
    sio.emit("subscribe", {"params": streams})
    print("Subscribed to:", streams)


@sio.event
def disconnect():
    print(f"[{datetime.now(IST).strftime('%H:%M:%S')}] Disconnected from Pi42. Auto-reconnecting...")


@sio.on("aggTrade")
def on_trade(data):
    try:
        qty = float(data.get("q", 0.0))
        is_buyer_maker = bool(data.get("m", False))
        signed_qty = -qty if is_buyer_maker else qty  # Positive = aggressive buy
        recent_trades.append((time.time(), signed_qty))
    except Exception:
        pass


@sio.on("depthUpdate")
def on_depth(data):
    global last_sample_ts
    now = time.time()
    if now - last_sample_ts < SAMPLE_INTERVAL_SEC:
        return

    bids = data.get("b", [])
    asks = data.get("a", [])
    if not bids or not asks:
        return

    try:
        best_bid, bid_q1 = float(bids[0][0]), float(bids[0][1])
        best_ask, ask_q1 = float(asks[0][0]), float(asks[0][1])
        if best_bid <= 0 or best_ask <= 0:
            return

        mid_price = (best_bid + best_ask) / 2.0
        spread_abs = best_ask - best_bid
        spread_pct = spread_abs / mid_price
        micro_price = (best_bid * ask_q1 + best_ask * bid_q1) / (bid_q1 + ask_q1 + 1e-9)

        obi_1, bv_1, av_1 = compute_obi(bids, asks, 1)
        obi_5, bv_5, av_5 = compute_obi(bids, asks, 5)
        obi_10, _, _ = compute_obi(bids, asks, 10)
        obi_20, bv_20, av_20 = compute_obi(bids, asks, 20)

        cutoff = now - 30.0
        cvd_30s = sum(q for t, q in recent_trades if t >= cutoff)

        buffer_rows.append({
            "ts_ms": int(now * 1000),
            "timestamp_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": CONTRACT_PAIR,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": round(mid_price, 4),
            "micro_price": round(micro_price, 4),
            "spread_pct": round(spread_pct, 6),
            "obi_1": round(obi_1, 6),
            "obi_5": round(obi_5, 6),
            "obi_10": round(obi_10, 6),
            "obi_20": round(obi_20, 6),
            "bid_vol_5": round(bv_5, 4),
            "ask_vol_5": round(av_5, 4),
            "bid_vol_20": round(bv_20, 4),
            "ask_vol_20": round(av_20, 4),
            "cvd_30s": round(cvd_30s, 4),
        })
        last_sample_ts = now
    except Exception as exc:
        print("Depth parse error:", exc)


def shutdown_handler(sig, frame):
    global running
    print("\nStopping collector and saving buffered data...")
    running = False
    flush_to_disk()
    try:
        sio.disconnect()
    except Exception:
        pass
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def main():
    print(f"Starting Pi42 L2 OBI Collector for {CONTRACT_PAIR}...")
    while running:
        try:
            if not sio.connected:
                sio.connect(PI42_WS_URL, transports=["websocket"])
            if time.time() - last_flush_ts >= FLUSH_INTERVAL_SEC:
                flush_to_disk()
            time.sleep(1)
        except Exception as exc:
            print(f"Connection/loop error: {exc}. Retrying in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    main()
