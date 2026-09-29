"""
dashboard.py - NKG coffee trading & risk dashboard
STUDENT PROJECT - non-commercial. All data is SYNTHETIC demo data, not real trading data.

What this file does, in plain English:
  Reads the finished tables that dbt built (the "marts") from the local DuckDB file
  and shows them on one page: headline P&L and exposure numbers, the futures price
  history, containers per port hub, and EUDR compliance. Two small icons near the
  top (a book and a bell) hide the explanatory text - hover the book for "how to
  read this page", hover the bell for anything that needs a second look. It
  refreshes itself every 60 seconds.

Run it from the project root:
    uv run streamlit run dashboard.py

It only READS the database, and opens a fresh short-lived connection for each query,
so the loader and dbt can still write between refreshes. If a write is in progress
right when a visitor loads the page, they see a short "retrying" message instead of
a blank page; if it still fails after a few retries, they see a message asking them
to contact the developer, with an email address, instead of a broken-looking page.

--------------------------------------------------------------------------------------
HOW THIS FILE IS ORGANISED (read this before making layout changes)
--------------------------------------------------------------------------------------
1. TOOLTIPS      - one dictionary with the plain-English explanation for every number.
                    Edit the TEXT here; nothing else needs to change.
2. CONFIGURATION - things you might want to tune (refresh time, page title, colours).
3. HELPERS       - small functions the page uses; you won't need to touch these often.
4. PAGE          - the actual layout, split into lettered BLOCKS (A, B, C, ...).
                    Each block is one visual section. To move a section, cut its whole
                    "# --- BLOCK ... ---" to "# --- END BLOCK ---" chunk and paste it
                    elsewhere in render_dashboard(). Blocks don't depend on each other's
                    order, only on `data`, `market` and `total`, which are loaded once
                    at the top of render_dashboard().

STREAMLIT LAYOUT CHEAT SHEET (the parts you'll use most for "move / resize" requests)
- st.columns(3)              -> 3 equal side-by-side boxes. st.columns([2, 1]) makes the
                                 first one twice as wide as the second.
- st.metric(label, value,    -> one big number with a small label above it and an
            help="...")         optional (?) icon that shows `help` text on hover.
- st.subheader("text")       -> a section heading.
- st.bar_chart(df) / .line_chart(df)  -> quick charts straight from a dataframe.
- st.dataframe(df)           -> a scrollable table.
- To change the ORDER things appear in: just move the block up or down in the code -
  Streamlit draws things top-to-bottom in the order you call them.
- To change a NUMBER FORMAT (decimals, currency, %): edit the f-string inside the
  block, e.g. f"{value:.1f}%" -> f"{value:.2f}%" for two decimal places.
--------------------------------------------------------------------------------------
"""

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

# Chart timing: the loop is meant to run only during a German working day (see
# refresh_loop.py), so the price chart stretches its x-axis so "now" always
# lands at CHART_NOW_FRACTION of the chart's width - close to but a little
# right of centre, so there's still visible room to the right for the rest of
# the working day, and hovering the most recent (most interesting) point
# naturally happens on the right-of-centre side of the chart rather than
# jammed against the edge.
#
# BUSINESS_TZ / WORKDAY_START_HOUR are duplicated from generate_futures_prices.py
# rather than imported, because this file is deliberately decoupled from the
# loader's Python modules (the dashboard should still run even if the loader
# code changes shape). If you change the working day's start time in the
# loader, change WORKDAY_START_HOUR here to match.
CHART_NOW_FRACTION = 0.70
BUSINESS_TZ = ZoneInfo("Europe/Berlin")
WORKDAY_START_HOUR = 8
CHART_MIN_DOMAIN_MINUTES = 30  # keeps the very first minutes of the day from looking absurdly zoomed-in

# ============================================================================
# 1) TOOLTIPS - the plain-English explanation behind every number on the page.
#    Written for a first-time trade-support hire: no jargon left unexplained,
#    but not a lecture either. Edit the TEXT below any time; the numbers
#    that use these are built in block C, E, F and G.
# ============================================================================
TOOLTIPS = {
    "total_mtm_eur": (
        "If we closed every position right now at today's market price, this is what "
        "we'd gain or lose overall, in euros. 'MtM' = mark-to-market: revaluing what "
        "we hold at today's price instead of the price we traded it at."
    ),
    "physical_mtm_eur": (
        "The gain or loss on the real, physical coffee only (bags actually bought or "
        "sold) - no futures hedges included. Buys gain when the price goes up; sells "
        "gain when the price goes down."
    ),
    "futures_mtm_eur": (
        "The gain or loss on our futures hedges only (paper contracts, no physical "
        "coffee). Hedges are meant to move opposite to the physical book, so this "
        "number often has the opposite sign to Physical MtM P&L."
    ),
    "net_unhedged_bags": (
        "Bags bought minus bags sold minus bags already protected by a futures hedge. "
        "This is the part of our position with NO price protection - if the market "
        "moves, we feel the full impact on exactly this many bags."
    ),
    "hedge_ratio": (
        "What share of our physical position is covered by a hedge. 100% means fully "
        "hedged. Below 100% means we're under-hedged (exposed); above 100% means "
        "we've hedged more than we need to."
    ),
    "price_move_impact": (
        "A quick stress test: if the market price moved by 10 USD per bag tomorrow, "
        "this is roughly how many euros we'd gain or lose, given today's unhedged bags."
    ),
    "futures_price": (
        "The current price used to value everything, in USD per 60kg bag. In this "
        "project it's a SIMULATED price standing in for a real ICE futures price, "
        "since real live prices need a paid market-data licence."
    ),
    "fx_rate": (
        "How many euros one US dollar buys right now. All the P&L numbers above are "
        "calculated in USD first, then converted to EUR using this rate."
    ),
    "pnl_by_origin_chart": (
        "The same Physical / Futures split as the top numbers, but broken down by "
        "where the coffee comes from - useful for spotting which origin is driving "
        "a gain or loss."
    ),
    "price_history_chart": (
        "How the simulated futures price has moved today, from the start of the "
        "working day to now. Hover the line to see the exact price at a given "
        "time (to the minute). A rising line means recent positions are worth "
        "more than when they were traded."
    ),
    "exposure_table": (
        "The raw building blocks behind the exposure numbers above, broken down by "
        "origin: how much was bought, how much was sold, and how much of the leftover "
        "position is hedged versus still exposed."
    ),
    "containers_chart": (
        "How many shipping containers are sitting at each stage of the journey (booked, "
        "in transit, arrived, in customs, delivered), grouped by which port hub they're "
        "headed to. A pile-up in one stage can mean a bottleneck."
    ),
    "containers_table": (
        "The same information as the chart above, but as a table you can scan row by "
        "row: one row per hub-and-stage combination, with the bag count and the "
        "earliest/latest expected arrival date for that group."
    ),
    "eudr_compliance_rate": (
        "The EU now requires proof that coffee didn't come from recently deforested "
        "land (the EUDR regulation). This is the share of our physical bags that "
        "already have that proof (a mapped farm 'polygon') on file."
    ),
    "eudr_pending_bags": (
        "Bags that do NOT yet have deforestation-free proof on file. These bags are "
        "at risk of being held up or rejected at EU customs until the paperwork is done."
    ),
    "eudr_pending_value": (
        "The euro value of those still-pending bags, at today's price - roughly how "
        "much trade value is sitting at risk until the compliance paperwork is done."
    ),
    "eudr_chart_by_origin": (
        "Compliance rate per origin, side by side, so you can see at a glance which "
        "origin has the most catching up to do on deforestation-free paperwork."
    ),
    "action_center": (
        "A short, automatically generated checklist of things worth a second look: "
        "exposure that isn't hedged, paperwork that's behind, shipments that are "
        "overdue, or data that looks stale. It only flags things for a person to "
        "review - it never makes a trading or logistics decision on its own."
    ),
    "risk_section": (
        "The desk's profit-and-loss and exposure numbers, revalued at today's "
        "market price and converted to EUR. This is the section a trader would "
        "check first thing each day."
    ),
    "dashboard_guide": (
        "**MtM (mark-to-market) P&L:** today's value of each position minus the "
        "price it was traded at. Buys gain when the price rises; sells and short "
        "hedges gain when it falls. Converted to EUR with the latest rate.\n\n"
        "**Net unhedged exposure:** bags bought minus bags sold minus bags covered "
        "by a futures hedge.\n\n"
        "**Hedge ratio:** hedged bags as a share of the net physical position.\n\n"
        "**EUDR compliance rate:** verified bags divided by all physical bags.\n\n"
        "**Synthetic data:** trades, containers and the ICE-style futures price "
        "are simulated for a student project."
    ),
}


# ============================================================================
# 2) CONFIGURATION - safe to tune without understanding the rest of the file.
# ============================================================================
DB_PATH = Path(os.environ.get("NKG_DB_PATH", "nkg_trading.duckdb")).resolve()
REFRESH_SECONDS = 60  # how often the page re-reads the database
STALE_AFTER_MINUTES = 30  # warn if the newest futures price is older than this
DB_LOCK_MAX_ATTEMPTS = 5  # how many times to retry a query if the file is briefly locked by a write
DB_LOCK_RETRY_SECONDS = 1  # pause between retries - 5 attempts = up to ~5 s before giving up
STATUS_ORDER = [  # left-to-right order for the container-stage chart
    "Booked at Origin",
    "In Transit",
    "Arrived at Hub",
    "Customs Clearance",
    "Delivered to Warehouse",
]
PAGE_TITLE = "NKG Coffee Risk Dashboard"
PAGE_ICON = "☕"
CONTACT_EMAIL = "govindarajan.senthilkumar.id@gmail.com"  # shown to a visitor only if the page can't recover on its own

st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")

# Pulls the whole page up closer to the browser tab bar (Streamlit's default
# top padding is fairly tall). Adjust the rem value if you want more/less gap.
st.markdown("<style>div.block-container{padding-top:2rem;}</style>", unsafe_allow_html=True)


# ============================================================================
# 3) HELPERS - you generally won't need to edit these for a layout change.
# ============================================================================
class DatabaseBusyError(Exception):
    """The database stayed locked (a write in progress) through every retry."""


def eur(value):
    """Format a number as euros with a leading minus sign, e.g. -€170,142."""
    sign = "-" if value < 0 else ""
    return f"{sign}€{abs(value):,.0f}"


def workday_start_utc(now_utc):
    """
    Today's 08:00 Europe/Berlin, as a naive UTC timestamp. Duplicated from
    generate_futures_prices.py (see the note by CHART_NOW_FRACTION above).
    If 'now' is before 08:00 Berlin, returns 'now' itself - there's no
    working day to stretch across yet.
    """
    now_aware = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    now_berlin = now_aware.astimezone(BUSINESS_TZ)
    start_berlin = now_berlin.replace(hour=WORKDAY_START_HOUR, minute=0, second=0, microsecond=0)
    if now_berlin < start_berlin:
        start_berlin = now_berlin
    return start_berlin.astimezone(timezone.utc).replace(tzinfo=None)


def render_price_history_chart(prices_df, now_utc):
    """
    Builds the live futures-price line chart. Two things this is designed to do
    that a plain st.line_chart can't:

    1. Keep "now" pinned at CHART_NOW_FRACTION (70%) of the chart's width, at
       any time of day. The x-axis domain is stretched from the start of the
       working day to a computed "domain_end" that is further out than "now" -
       specifically domain_end = workday_start + elapsed / CHART_NOW_FRACTION.
       Since elapsed / (elapsed / CHART_NOW_FRACTION) = CHART_NOW_FRACTION by
       construction, "now" always lands at that exact fraction of the axis,
       whether it's 8:05am or 4:55pm, leaving open space to the right for the
       rest of the day. A floor (CHART_MIN_DOMAIN_MINUTES) stops the domain
       from being a near-zero sliver in the first minute of the day.

    2. Show a minute-precise tooltip on hover, via a layered chart: the visible
       line, an invisible layer of point marks used only to detect the nearest
       point to the mouse (alt.selection_point(nearest=True)), a highlight
       circle + rule that appear at that point, and a tooltip formatted with
       "%H:%M" (hours:minutes) rather than the date-only default.

    One honest limitation: no charting library (Altair/Vega-Lite included) can
    force a tooltip to always render at a fixed screen position - a tooltip
    always follows the mouse cursor, by design, in every standard library.
    What (1) actually achieves is different but delivers the same practical
    effect: it keeps "now" - the point anyone would actually hover for the
    live value - sitting at 60-80% of the chart's width, so hovering the one
    point that matters naturally happens in that zone rather than at the
    far-right edge.
    """
    df = prices_df.rename(columns={"price_timestamp": "timestamp", "price_usd_per_bag": "price"}).copy()

    now_naive = now_utc.astimezone(timezone.utc).replace(tzinfo=None) if now_utc.tzinfo is not None else now_utc
    domain_start = workday_start_utc(now_naive)
    elapsed_seconds = max((now_naive - domain_start).total_seconds(), 0)
    stretched_seconds = elapsed_seconds / CHART_NOW_FRACTION if elapsed_seconds > 0 else 0
    domain_end = domain_start + timedelta(seconds=stretched_seconds)
    min_end = domain_start + timedelta(minutes=CHART_MIN_DOMAIN_MINUTES)
    if domain_end < min_end:
        domain_end = min_end

    x_scale = alt.Scale(domain=[domain_start, domain_end])
    x_enc = alt.X("timestamp:T", scale=x_scale, title="Time", axis=alt.Axis(format="%H:%M"))
    y_enc = alt.Y("price:Q", title="USD / bag", scale=alt.Scale(zero=False))

    line = alt.Chart(df).mark_line(color="#1f77b4").encode(x=x_enc, y=y_enc)

    nearest = alt.selection_point(nearest=True, on="mouseover", fields=["timestamp"], empty=False)
    selectors = (
        alt.Chart(df)
        .mark_point(opacity=0)
        .encode(
            x=x_enc,
            tooltip=[
                alt.Tooltip("timestamp:T", title="Time", format="%H:%M"),
                alt.Tooltip("price:Q", title="USD / bag", format=".2f"),
            ],
        )
        .add_params(nearest)
    )
    highlight = line.mark_circle(size=70, color="#1f77b4").encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )
    hover_rule = (
        alt.Chart(df)
        .mark_rule(color="lightgray")
        .encode(x=x_enc)
        .transform_filter(nearest)
    )
    now_rule = (
        alt.Chart(pd.DataFrame({"now": [now_naive]}))
        .mark_rule(color="lightgray", strokeDash=[4, 4])
        .encode(x=alt.X("now:T", scale=x_scale))
    )

    chart = (now_rule + line + selectors + hover_rule + highlight).properties(height=300)
    st.altair_chart(chart, use_container_width=True)


def run_query(sql, _status=None):
    """
    Open the DB read-only, run one query, close it. If the loader or dbt is
    mid-write, the file is briefly locked - this retries a few times, and if
    a placeholder was given (_status), it updates that placeholder so the
    VISITOR sees why the page is taking a moment, instead of a blank screen
    or a raw error. Raises DatabaseBusyError only if every attempt fails.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database file not found: {DB_PATH}")
    last_error = None
    for attempt in range(1, DB_LOCK_MAX_ATTEMPTS + 1):
        try:
            con = duckdb.connect(str(DB_PATH), read_only=True)
            try:
                return con.execute(sql).df()
            finally:
                con.close()
        except duckdb.IOException as exc:  # file is locked by the loader or dbt
            last_error = exc
            if _status is not None and attempt < DB_LOCK_MAX_ATTEMPTS:
                _status.info(
                    f"⏳ Updating the numbers — a background data refresh is in progress. "
                    f"Retrying ({attempt}/{DB_LOCK_MAX_ATTEMPTS})…"
                )
            time.sleep(DB_LOCK_RETRY_SECONDS)
    raise DatabaseBusyError(
        f"The database stayed locked after {DB_LOCK_MAX_ATTEMPTS} attempts"
    ) from last_error


@st.cache_data(ttl=30, show_spinner=False)
def load_data(_status=None):
    """
    All the tables the page needs. Cached for 30 s so refreshes stay cheap.
    _status: an optional st.empty() placeholder, forwarded to every query so
    a lock retry can update it (leading underscore = Streamlit's cache_data
    does not try to hash it, so passing a UI element here is safe).
    """
    return {
        "market": run_query("select * from mart_latest_market", _status),
        "total": run_query("select * from mart_risk_total", _status),
        "by_origin": run_query("select * from mart_risk_by_origin order by origin", _status),
        "tracking": run_query("select * from mart_container_tracking", _status),
        "eudr": run_query("select * from mart_eudr_compliance", _status),
        "prices": run_query(
            "select price_timestamp, price_usd_per_bag from stg_futures_prices order by price_timestamp", _status
        ),
        "flags": run_query("select * from mart_review_flags", _status),
    }


def build_flags_list(flags_df, market, age_minutes):
    """
    Turns the dbt flags table into a plain Python list of dicts, and adds two
    more flags that depend on 'right now' (data freshness, live vs fallback
    FX) rather than on stored data, so those live here instead of in dbt.
    Every flag is a dict with: severity, category, title, why_it_matters,
    suggested_action, scope, metric_text. To add a new "live" flag, append
    another dict with the same keys.
    """
    flags = []

    for row in flags_df.itertuples(index=False):
        flags.append(
            {
                "severity": row.severity,
                "category": row.category,
                "title": row.title,
                "why_it_matters": row.why_it_matters,
                "suggested_action": row.suggested_action,
                "scope": row.scope,
                "metric_text": f"{row.metric_value:,.0f} {row.metric_unit}",
            }
        )

    if age_minutes > STALE_AFTER_MINUTES:
        flags.append(
            {
                "severity": "High" if age_minutes > STALE_AFTER_MINUTES * 3 else "Medium",
                "category": "Data quality",
                "title": "Data pipeline may have stopped",
                "why_it_matters": "Every number on this page is only as current as the last successful data load. Stale data can lead to a decision based on an out-of-date position.",
                "suggested_action": "Confirm the data loader is still running on its schedule.",
                "scope": "Whole dashboard",
                "metric_text": f"{age_minutes:,.0f} minutes old",
            }
        )

    if market["fx_source"] != "live":
        flags.append(
            {
                "severity": "Medium",
                "category": "Data quality",
                "title": "Using fallback FX rates",
                "why_it_matters": "The live exchange-rate source was unavailable, so a fixed backup rate was used instead. Every EUR figure on this page is only approximate until live rates return.",
                "suggested_action": "Treat EUR figures as indicative until the FX data source is confirmed to be back online.",
                "scope": "Whole dashboard",
                "metric_text": "fallback rate in use",
            }
        )

    severity_order = {"High": 0, "Medium": 1, "Low": 2}
    flags.sort(key=lambda f: severity_order.get(f["severity"], 9))
    return flags


# ============================================================================
# 4) PAGE - the visual layout. Organised into lettered blocks (see file header).
# ============================================================================
@st.fragment(run_every=REFRESH_SECONDS)
def render_dashboard():
    # --- BLOCK A: load data, with friendly messages for the common problems ---
    # status_box shows a live "retrying" message if the file is briefly locked by
    # a write (the loader or dbt running), and is cleared again the moment data
    # loads successfully - a visitor should see WHY the page is taking a moment,
    # never a blank page or a raw error. If every retry fails, or something else
    # goes wrong, the message tells them to contact the developer instead of
    # just breaking - important on a public dashboard nobody is watching 24/7.
    status_box = st.empty()
    try:
        data = load_data(status_box)
    except FileNotFoundError as exc:
        status_box.error(
            f"{exc}. Run the loader first: uv run python generate_trades.py. "
            f"If you're seeing this on the public dashboard, please contact {CONTACT_EMAIL}."
        )
        return
    except DatabaseBusyError:
        status_box.error(
            "⚠️ The dashboard couldn't refresh because a background data update was still in "
            "progress after several attempts. This is usually temporary - try refreshing the page "
            f"in a minute. If it keeps happening, please contact {CONTACT_EMAIL}."
        )
        return
    except duckdb.CatalogException:
        status_box.error(
            "The dbt models have not been built yet. Run: cd dbt_nkg  then  uv run dbt build. "
            f"If you're seeing this on the public dashboard, please contact {CONTACT_EMAIL}."
        )
        return
    except duckdb.Error as exc:
        status_box.error(
            f"Could not read the database ({type(exc).__name__}). If this keeps happening, "
            f"please contact {CONTACT_EMAIL}."
        )
        return
    else:
        status_box.empty()  # clear any "retrying..." message left over from a successful retry

    if data["total"].empty or data["market"].empty:
        st.info("No data yet. Run the loader, then dbt, then this page will fill in.")
        return

    market = data["market"].iloc[0]
    total = data["total"].iloc[0]
    price_ts = market["price_timestamp_utc"]
    age_minutes = (datetime.now(timezone.utc).replace(tzinfo=None) - price_ts.to_pydatetime()).total_seconds() / 60
    flags = build_flags_list(data["flags"], market, age_minutes)
    # --- END BLOCK A ---

    # --- BLOCK A2: two icons, top-right - everything explanatory hides behind these ---
    # ℹ️ = "how to read this dashboard" (was a collapsible glossary; now a hover tooltip).
    # 🔔 = "what needs review" (was a list of alert boxes; now a hover tooltip with a count).
    # Both use st.markdown's built-in (?) tooltip trigger - the visible label is just the
    # emoji, the explanation only appears on hover. To move these elsewhere on the page,
    # cut this whole block and paste it next to any other subheader.
    _spacer, guide_col, bell_col = st.columns([14, 1, 1])
    with guide_col:
        st.markdown("ℹ️", help=TOOLTIPS["dashboard_guide"])
    with bell_col:
        SEVERITY_ICON = {"High": "🔴", "Medium": "🟠", "Low": "🟡"}
        if flags:
            entries = [
                f"{SEVERITY_ICON.get(f['severity'], '⚪')} **{f['title']}** — {f['scope']} "
                f"({f['metric_text']}). {f['why_it_matters']} *Next: {f['suggested_action']}*"
                for f in flags
            ]
            bell_text = "\n\n".join(entries)
            bell_label = f"🔔 {len(flags)}"
        else:
            bell_text = "Nothing needs review right now - exposure, compliance and logistics checks are all within range."
            bell_label = "🔔"
        st.markdown(bell_label, help=bell_text)
    # --- END BLOCK A2 ---

    # --- BLOCK B: header strip - data freshness, futures price, FX rate ---
    st.caption(
        f"Simulated ICE-style futures price: **{market['futures_price_usd']:.2f} USD/bag** "
        f"(as of {price_ts:%Y-%m-%d %H:%M} UTC) · USD→EUR **{market['fx_usd_eur']:.4f}** "
        f"({market['fx_source']} rate) · page refreshes every {REFRESH_SECONDS} s"
    )
    # --- END BLOCK B ---

    # --- BLOCK C: headline risk metrics (2 rows x 3 columns) ---
    # To resize: st.columns(3) -> st.columns([2, 1, 1]) makes the first box wider.
    # To reorder: just move the c1/c2/c3 lines around, or move this whole block.
    st.subheader("Risk: MtM P&L and exposure (reporting currency EUR)", help=TOOLTIPS["risk_section"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Total MtM P&L", eur(total["total_mtm_eur"]), help=TOOLTIPS["total_mtm_eur"])
    c2.metric("Physical MtM P&L", eur(total["physical_mtm_eur"]), help=TOOLTIPS["physical_mtm_eur"])
    c3.metric("Futures MtM P&L", eur(total["futures_mtm_eur"]), help=TOOLTIPS["futures_mtm_eur"])

    c4, c5, c6 = st.columns(3)
    c4.metric(
        "Net unhedged exposure (bags)",
        f"{total['net_unhedged_bags']:,.0f}",
        help=TOOLTIPS["net_unhedged_bags"],
    )
    hedge_ratio = total["hedge_ratio_pct"]
    c5.metric(
        "Hedge ratio",
        f"{hedge_ratio:.0f}%" if pd.notna(hedge_ratio) else "n/a (book not net long)",
        help=TOOLTIPS["hedge_ratio"],
    )
    c6.metric(
        "EUR impact of a 10 USD price move",
        eur(total["eur_impact_of_10usd_price_move"]),
        help=TOOLTIPS["price_move_impact"],
    )
    # --- END BLOCK C ---

    # --- BLOCK D: two side-by-side charts (P&L by origin, price history) ---
    # To stack them instead of side-by-side: remove the st.columns(2) and the
    # `with left:` / `with right:` lines, and just call st.markdown + st.bar_chart
    # / st.line_chart one after another.
    left, right = st.columns(2)
    with left:
        st.markdown("**MtM P&L by origin (EUR)**", help=TOOLTIPS["pnl_by_origin_chart"])
        pnl = data["by_origin"].set_index("origin")[["physical_mtm_eur", "futures_mtm_eur"]]
        pnl.columns = ["Physical", "Futures"]
        st.bar_chart(pnl)
    with right:
        st.markdown("**Simulated futures price history (USD/bag)**", help=TOOLTIPS["price_history_chart"])
        render_price_history_chart(data["prices"], datetime.now(timezone.utc))
    # --- END BLOCK D ---

    # --- BLOCK E: exposure table by origin ---
    st.markdown("**Exposure by origin (bags)**", help=TOOLTIPS["exposure_table"])
    exposure = data["by_origin"][
        ["origin", "physical_bought_bags", "physical_sold_bags", "net_physical_bags", "futures_hedge_bags", "net_unhedged_bags"]
    ]
    st.dataframe(exposure, hide_index=True)
    # --- END BLOCK E ---

    # --- BLOCK F: logistics - containers per port hub and stage (collapsed by default) ---
    st.subheader("Logistics: containers by port hub and stage", help=TOOLTIPS["containers_chart"])
    with st.expander("Show container details"):
        tracking = data["tracking"]
        pivot = tracking.pivot_table(
            index="destination_hub", columns="status", values="containers", aggfunc="sum", fill_value=0
        )
        ordered = [s for s in STATUS_ORDER if s in pivot.columns]
        st.bar_chart(pivot[ordered])
        st.markdown("**Containers detail**", help=TOOLTIPS["containers_table"])
        st.dataframe(
            tracking.sort_values(["destination_hub", "status_order"])[
                ["destination_hub", "status", "containers", "bags", "earliest_eta", "latest_eta"]
            ],
            hide_index=True,
        )
    # --- END BLOCK F ---

    # --- BLOCK G: EUDR compliance (collapsed by default) ---
    st.subheader("EUDR deforestation compliance (physical trades, by bags)")
    with st.expander("Show EUDR breakdown"):
        eudr = data["eudr"]
        overall = eudr[eudr["origin"] == "ALL ORIGINS"].iloc[0]

        e1, e2, e3 = st.columns(3)
        e1.metric(
            "Overall compliance rate",
            f"{overall['compliance_rate_pct']:.1f}%",
            help=TOOLTIPS["eudr_compliance_rate"],
        )
        e2.metric(
            "Bags pending polygon mapping",
            f"{overall['pending_bags']:,.0f}",
            help=TOOLTIPS["eudr_pending_bags"],
        )
        e3.metric("Value of pending bags", eur(overall["pending_value_eur"]), help=TOOLTIPS["eudr_pending_value"])

        by_origin = eudr[eudr["origin"] != "ALL ORIGINS"].set_index("origin")
        st.markdown("**Compliance rate by origin (%)**", help=TOOLTIPS["eudr_chart_by_origin"])
        st.bar_chart(by_origin[["compliance_rate_pct"]].rename(columns={"compliance_rate_pct": "Compliance rate %"}))
    # --- END BLOCK G ---


# --- BLOCK Z: page title strip (runs once, above the auto-refreshing part) ---
st.title("NKG Coffee Trading & Risk Dashboard")
render_dashboard()
# --- END BLOCK Z ---
