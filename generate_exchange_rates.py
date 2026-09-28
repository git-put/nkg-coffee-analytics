"""
generate_exchange_rates.py  (STUDENT PROJECT - non-commercial, educational use only)

What this file does, in plain English:
  Asks a free public API (open.er-api.com) for today's USD->EUR and USD->BRL rates.
  If the API is down, slow, or returns nonsense, it falls back to fixed rates and
  clearly labels the result as "fallback" so nobody mistakes it for live data.

It is used in two ways:
  1. Imported by generate_trades.py (that is the normal way).
  2. Run on its own to test just the FX part:   python generate_exchange_rates.py

It also holds the small print helpers ([OK] / [WARN] / [FAIL] ...) that
generate_trades.py re-uses, so both files log in exactly the same style.
"""

import math
import sys
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------------------------------------------------------
# CONFIGURATION (everything you might want to change lives here)
# ----------------------------------------------------------------------------
CONTACT_EMAIL = "govindarajan.senthilkumar.id@gmail.com"
PROJECT_PURPOSE = "Student project - educational use only, nothing commercial"

FX_API_URL = "https://open.er-api.com/v6/latest/USD"
FX_TIMEOUT_SECONDS = (5, 15)  # (connect timeout, read timeout)
FALLBACK_FX = {"USD_BRL": 5.25, "USD_EUR": 0.92}
# Sanity bands: a live rate outside these is treated as a bad API response.
FX_SANITY_RANGE = {"USD_BRL": (1.0, 20.0), "USD_EUR": (0.3, 2.0)}


# ----------------------------------------------------------------------------
# PRINT HELPERS (consistent format so logs are easy to read and search)
# ----------------------------------------------------------------------------
def _stamp():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def section(title):
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def info(msg):
    print(f"[{_stamp()}] [INFO ] {msg}")


def ok(msg):
    print(f"[{_stamp()}] [OK   ] {msg}")


def warn(msg):
    print(f"[{_stamp()}] [WARN ] {msg}")


def fail(msg):
    print(f"[{_stamp()}] [FAIL ] {msg}")


# ----------------------------------------------------------------------------
# SECTION 1 - FETCH FX RATES
# ----------------------------------------------------------------------------
def build_http_session():
    """Session that identifies us politely and retries temporary failures."""
    retry = Retry(
        total=3,
        backoff_factor=1,  # waits 1s, 2s, 4s between attempts
        status_forcelist=(429, 500, 502, 503, 504),  # rate-limit + server hiccups
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(
        {
            "User-Agent": f"NKG-Coffee-Trades-Student-Project/1.0 ({PROJECT_PURPOSE}; contact: {CONTACT_EMAIL})",
            "From": CONTACT_EMAIL,
            "Accept": "application/json",
        }
    )
    return session


def validate_rate(pair, raw_value):
    """Return (clean_float, None) if usable, else (None, reason_it_is_bad)."""
    if raw_value is None:
        return None, "missing from API response"
    if isinstance(raw_value, bool):
        return None, f"is a boolean ({raw_value!r}), not a number"
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None, f"is not numeric ({raw_value!r})"
    if not math.isfinite(value) or value <= 0:
        return None, f"is not a positive finite number ({value})"
    low, high = FX_SANITY_RANGE[pair]
    if not low <= value <= high:
        return None, f"= {value} is outside the plausible range {low}-{high}"
    return value, None


def use_fallback_fx(reason):
    warn(f"Using FALLBACK FX rates because: {reason}")
    warn(f"Fallback values: {FALLBACK_FX} (rows will be labelled fx_source='fallback')")
    return {**FALLBACK_FX, "source": "fallback"}


def fetch_live_fx_rates():
    """
    Returns {"USD_BRL": float, "USD_EUR": float, "source": "live" | "fallback"}.
    Never raises: any problem leads to a clearly-labelled fallback.
    """
    section("SECTION 1 - Fetching live FX rates")
    info(f"Calling {FX_API_URL}")
    info(f"Identifying ourselves as: {PROJECT_PURPOSE} (contact: {CONTACT_EMAIL})")

    try:
        session = build_http_session()
        response = session.get(FX_API_URL, timeout=FX_TIMEOUT_SECONDS)
    except requests.exceptions.Timeout:
        return use_fallback_fx(f"request timed out after {FX_TIMEOUT_SECONDS}s")
    except requests.exceptions.ConnectionError as exc:
        return use_fallback_fx(f"could not connect (no internet / DNS / firewall?): {exc}")
    except requests.exceptions.RequestException as exc:
        return use_fallback_fx(f"request failed after retries: {exc}")

    info(f"HTTP status received: {response.status_code}")
    if response.status_code != 200:
        return use_fallback_fx(f"API answered with HTTP {response.status_code}")

    try:
        data = response.json()
    except ValueError:
        return use_fallback_fx("response was not valid JSON")
    ok("Response is valid JSON")

    if not isinstance(data, dict) or data.get("result") != "success":
        result = data.get("result") if isinstance(data, dict) else type(data).__name__
        return use_fallback_fx(f"API 'result' field was {result!r}, expected 'success'")
    ok(f"API reports success (rates last updated by provider: {data.get('time_last_update_utc', 'unknown')})")

    rates = data.get("rates")
    if not isinstance(rates, dict):
        return use_fallback_fx("'rates' section missing or malformed")

    clean = {}
    for pair, currency in (("USD_BRL", "BRL"), ("USD_EUR", "EUR")):
        value, problem = validate_rate(pair, rates.get(currency))
        if problem:
            return use_fallback_fx(f"{pair} {problem}")
        ok(f"{pair} = {value} passed validation")
        clean[pair] = value

    ok("Both live rates are valid - using LIVE FX")
    return {**clean, "source": "live"}




def main():
    """Stand-alone test: fetch the rates and print them."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    fx = fetch_live_fx_rates()
    section("RESULT")
    ok(f"USD_EUR={fx['USD_EUR']}  USD_BRL={fx['USD_BRL']}  source={fx['source']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
