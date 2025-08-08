# app.py
import streamlit as st
import pandas as pd
import requests
import threading
import time
from datetime import datetime, timedelta, timezone
from dateutil import tz

st.set_page_config(page_title="Dhan OCO — Auto Place (Buy+Sell)", layout="wide")

# --------- Config / constants ----------
DHAN_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
# NOTE: update these endpoints to the exact Dhan endpoints from docs if they differ
QUOTE_URL = "https://api.dhan.co/v1/market/quote"
PLACE_ORDER_URL = "https://api.dhan.co/v1/orders"
CANCEL_ORDER_URL = "https://api.dhan.co/v1/orders/{orderId}/cancel"
MODIFY_ORDER_URL = "https://api.dhan.co/v1/orders/{orderId}"

# --------- Helpers ----------
def dh_headers(access_token):
    return {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

def download_instruments(url=DHAN_CSV_URL, timeout=10):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    # read into pandas
    from io import StringIO
    df = pd.read_csv(StringIO(r.text), dtype=str)
    # normalize columns
    cols = {c.lower(): c for c in df.columns}
    # pick tradingsymbol
    if 'tradingsymbol' in cols:
        df['tradingsymbol_norm'] = df[cols['tradingsymbol']]
    elif 'symbol' in cols:
        df['tradingsymbol_norm'] = df[cols['symbol']]
    else:
        df['tradingsymbol_norm'] = df.iloc[:,0].astype(str)

    # pick security id candidates
    sec_col = None
    for candidate in ['secid','securityid','token','instrument_token','security_id','instrumentid','instrument_id']:
        if candidate in cols:
            sec_col = cols[candidate]
            break
    if sec_col:
        df['securityId_norm'] = df[sec_col].astype(str)
    else:
        df['securityId_norm'] = None
    df['tradingsymbol_norm_lc'] = df['tradingsymbol_norm'].str.lower()
    return df

def find_instrument(df, query):
    q = str(query).strip().lower()
    if q == "":
        return None
    # exact match
    mask = df['tradingsymbol_norm_lc'] == q
    if mask.any():
        return df[mask].iloc[0]
    # contains
    mask = df['tradingsymbol_norm_lc'].str.contains(q, na=False)
    if mask.any():
        return df[mask].iloc[0]
    return None

def get_quote(security_id, access_token):
    try:
        resp = requests.get(QUOTE_URL, params={"securityId": str(security_id)}, headers=dh_headers(access_token), timeout=8)
        if resp.status_code == 200:
            return resp.json()
        else:
            return {"error": f"quote_status_{resp.status_code}", "text": resp.text}
    except Exception as e:
        return {"error": str(e)}

def extract_last_price_from_quote(quote_json):
    # Dhan's quote structure varies; attempt robust extraction
    try:
        if not quote_json:
            return None
        if isinstance(quote_json, dict):
            # common pattern: {'data': {'lastPrice': ...}}
            data = quote_json.get('data') or quote_json
            if isinstance(data, dict):
                if 'lastPrice' in data:
                    return data.get('lastPrice')
                if 'last' in data:
                    return data.get('last')
            # Or nested per symbol
            for k,v in quote_json.items():
                if isinstance(v, dict):
                    if 'lastPrice' in v:
                        return v['lastPrice']
                    if 'last' in v:
                        return v['last']
        # fallback none
        return None
    except Exception:
        return None

def place_order(payload, access_token):
    try:
        r = requests.post(PLACE_ORDER_URL, json=payload, headers=dh_headers(access_token), timeout=10)
        try:
            return {"status_code": r.status_code, "resp": r.json() if r.content else r.text}
        except Exception:
            return {"status_code": r.status_code, "resp_text": r.text}
    except Exception as e:
        return {"status_code": "exception", "error": str(e)}

def cancel_order_api(order_id, access_token):
    try:
        url = CANCEL_ORDER_URL.format(orderId=order_id)
        r = requests.post(url, headers=dh_headers(access_token), timeout=8)
        return {"status_code": r.status_code, "resp": r.text}
    except Exception as e:
        return {"status_code": "exception", "error": str(e)}

def modify_order_api(order_id, payload, access_token):
    try:
        url = MODIFY_ORDER_URL.format(orderId=order_id)
        r = requests.patch(url, json=payload, headers=dh_headers(access_token), timeout=8)
        try:
            return {"status_code": r.status_code, "resp": r.json() if r.content else r.text}
        except Exception:
            return {"status_code": r.status_code, "resp_text": r.text}
    except Exception as e:
        return {"status_code": "exception", "error": str(e)}

def ist_now():
    # returns timezone-aware datetime in IST
    return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(tz.gettz("Asia/Kolkata"))

def next_market_open_utc():
    # Market open time 09:15 IST for today (or tomorrow if already past)
    now_ist = ist_now()
    today_open_ist = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    if now_ist >= today_open_ist:
        # tomorrow
        tomorrow = now_ist + timedelta(days=1)
        open_ist = tomorrow.replace(hour=9, minute=15, second=0, microsecond=0)
    else:
        open_ist = today_open_ist
    # convert to UTC naive or tz-aware
    return open_ist.astimezone(timezone.utc)

# --------- Streamlit UI ----------
st.title("Dhan OCO — Auto Place (Buy+Sell)")

col_a, col_b = st.columns([1,2])
with col_a:
    access_token = st.text_input("Dhan Access Token (paste daily)", type="password")
    if st.button("Download instruments from Dhan (recommended)"):
        try:
            df = download_instruments()
            st.session_state['instruments_df'] = df
            st.success("Downloaded instruments list.")
            st.dataframe(df[['tradingsymbol_norm','securityId_norm']].drop_duplicates().head(30))
        except Exception as e:
            st.error(f"Failed to download instruments CSV: {e}")
    uploaded_csv = st.file_uploader("Or upload instruments.csv manually", type=["csv"])
    if uploaded_csv is not None:
        try:
            df2 = pd.read_csv(uploaded_csv, dtype=str)
            # normalize similar to download_instruments
            df2 = download_instruments(url=DHAN_CSV_URL) if False else df2  # placeholder to avoid lint issue
            # use small helper to normalize
            # (we reuse the same normalization by writing a tiny utility)
            cols = {c.lower(): c for c in df2.columns}
            if 'tradingsymbol' in cols:
                df2['tradingsymbol_norm'] = df2[cols['tradingsymbol']]
            elif 'symbol' in cols:
                df2['tradingsymbol_norm'] = df2[cols['symbol']]
            else:
                df2['tradingsymbol_norm'] = df2.iloc[:,0].astype(str)
            sec_col = None
            for candidate in ['secid','securityid','token','instrument_token','security_id','instrumentid','instrument_id']:
                if candidate in cols:
                    sec_col = cols[candidate]
                    break
            if sec_col:
                df2['securityId_norm'] = df2[sec_col].astype(str)
            else:
                df2['securityId_norm'] = None
            df2['tradingsymbol_norm_lc'] = df2['tradingsymbol_norm'].str.lower()
            st.session_state['instruments_df'] = df2
            st.success("Uploaded instruments.csv loaded.")
        except Exception as e:
            st.error(f"Failed to load uploaded csv: {e}")

    st.markdown("---")
    st.write("**Auto-start settings**")
    auto_start_at_open = st.checkbox("Auto Start at Market Open (09:15 IST)", value=True)
    poll_interval = st.number_input("Quote poll interval (seconds)", min_value=1, value=2, step=1)
    st.write("**Safety**: background monitor auto-stops after 6 hours.")

with col_b:
    st.markdown("### Order inputs")
    symbol_input = st.text_input("Stock name / symbol (e.g. RELIANCE)")
    entry_price = st.number_input("Entry price", min_value=0.0, format="%.2f")
    qty = st.number_input("Quantity", min_value=1, value=1, step=1)
    order_type = st.selectbox("Order type at execution", ["MARKET", "LIMIT"])
    sl_percent = st.number_input("Stop Loss % (from entry)", min_value=0.0, format="%.2f", value=0.5)
    target_percent = st.number_input("Target % (from entry)", min_value=0.0, format="%.2f", value=1.0)
    tsl_percent = st.number_input("Trailing SL % (manual)", min_value=0.0, format="%.2f", value=0.5)
    st.markdown("**Note:** Trailing SL is manual — app will place initial SL. You can modify via Order ID after execution.")

st.markdown("---")

# Logs
if 'log' not in st.session_state:
    st.session_state['log'] = []

def append_log(msg):
    t = ist_now().strftime("%Y-%m-%d %H:%M:%S IST")
    st.session_state['log'].append(f"[{t}] {msg}")

# Monitoring thread function
def monitor_flow(symbol, sec_id, entry_price, order_type, qty, sl_pct, tgt_pct, access_tok, poll_interval, auto_start_at_open):
    # Wait until market open if requested
    if auto_start_at_open:
        append_log("Auto-start at market open enabled. Waiting for market open (09:15 IST).")
        target_utc = next_market_open_utc()
        while datetime.now(timezone.utc) < target_utc:
            remaining = (target_utc - datetime.now(timezone.utc)).total_seconds()
            if remaining % 60 < 1:
                append_log(f"Time to market open: {int(remaining//60)} minutes")
            time.sleep(min(5, remaining))
        append_log("Market open reached. Starting quote monitoring.")
    else:
        append_log("Starting immediate quote monitoring (no wait for market open).")

    start = time.time()
    timeout_seconds = 6 * 3600  # 6 hours safety
    placed = False

    while not placed and (time.time() - start) < timeout_seconds:
        q = get_quote(sec_id, access_tok)
        if isinstance(q, dict) and q.get("error"):
            append_log(f"Quote fetch error: {q.get('error')}")
            time.sleep(poll_interval)
            continue
        last = extract_last_price_from_quote(q)
        if last is None:
            append_log("Could not extract last price from quote; retrying.")
            time.sleep(poll_interval)
            continue
        try:
            last_f = float(last)
        except Exception:
            append_log("Non-numeric last price; retrying.")
            time.sleep(poll_interval)
            continue

        append_log(f"Tick: {last_f}")
        # decide trigger: if last >= entry => SELL trigger; if last <= entry => BUY trigger
        triggered_side = None
        if last_f >= float(entry_price):
            triggered_side = "SELL"
        elif last_f <= float(entry_price):
            triggered_side = "BUY"

        if triggered_side:
            append_log(f"Entry touched. Triggered side: {triggered_side}. Placing order...")
            # Build payload — adapt fields to Dhan API as necessary
            payload = {
                "exchangeSegment": "NSE",
                "symbol": symbol,
                "securityId": str(sec_id),
                "transactionType": triggered_side,
                "quantity": int(qty),
                "orderType": order_type,
                "productCode": "INTRADAY",
                "validity": "DAY",
                # placing price only if LIMIT
                **({"price": float(entry_price)} if order_type == "LIMIT" else {}),
                # attaching sl/target as percent fields (adapt if Dhan expects absolute prices)
                "stopLossPercent": float(sl_pct),
                "targetPercent": float(tgt_pct)
            }
            resp = place_order(payload, access_tok)
            append_log(f"Place order response: {resp}")
            # If response reports order id, show it. We'll stop monitoring either way.
            placed = True
            append_log(f"Stopped monitoring after placing {triggered_side} order.")
            break

        time.sleep(poll_interval)

    if not placed:
        append_log("Monitoring ended: no execution (timeout or stopped).")

# Controls to start/stop
col_c, col_d = st.columns([1,1])
with col_c:
    if st.button("Start Auto-Monitor & Place"):
        if 'instruments_df' not in st.session_state:
            st.error("Load instruments list first (download or upload).")
        elif not access_token:
            st.error("Provide Access Token.")
        elif not symbol_input or entry_price <= 0:
            st.error("Provide valid symbol and entry price.")
        else:
            df = st.session_state['instruments_df']
            found = find_instrument(df, symbol_input)
            if found is None:
                st.error("Symbol not found in instruments list. Check symbol or upload correct CSV.")
            else:
                sec_id = found.get('securityId_norm')
                sym = found.get('tradingsymbol_norm')
                append_log(f"Prepared to monitor {sym} (secId={sec_id}).")
                t = threading.Thread(target=monitor_flow, args=(
                    sym, sec_id, entry_price, order_type, qty, sl_percent, target_percent, access_token, poll_interval, auto_start_at_open
                ), daemon=True)
                t.start()
                st.success("Monitoring thread started (runs in background).")

with col_d:
    if st.button("Stop (Refresh to force stop)"):
        append_log("Stop requested. Refresh page to forcibly stop running threads if needed.")
        st.warning("To forcibly stop background threads, refresh the page.")

st.markdown("## Logs")
st.text_area("Activity log", value="\n".join(st.session_state['log'][-400:]), height=300)

st.markdown("---")
st.markdown("### Manual order actions (after execution) — cancel/modify")
order_id_input = st.text_input("Order ID (for modify/cancel)")
col_m1, col_m2 = st.columns(2)
with col_m1:
    if st.button("Cancel Order (API)"):
        if not order_id_input or not access_token:
            st.error("Provide Order ID and token.")
        else:
            res = cancel_order_api(order_id_input, access_token)
            append_log(f"Cancel API: {res}")
            st.success("Cancel API called; check logs.")
with col_m2:
    new_sl_abs = st.number_input("New SL (absolute price)", min_value=0.0, format="%.2f", key="new_sl_abs")
    if st.button("Modify SL (API)"):
        if not order_id_input or not access_token:
            st.error("Provide Order ID and token.")
        else:
            payload = {"stopLossPrice": float(new_sl_abs)}
            res = modify_order_api(order_id_input, payload, access_token)
            append_log(f"Modify API: {res}")
            st.success("Modify API called; check logs.")

st.markdown("**Important**: Verify payload & endpoints with Dhan docs. Test with small qty first.")
