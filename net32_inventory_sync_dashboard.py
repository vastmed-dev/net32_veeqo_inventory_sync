import os
import json
import hashlib
from datetime import datetime

import streamlit as st
import pandas as pd

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Net32 Inventory Sync Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# CUSTOM CSS
# =========================================================
st.markdown("""
<style>
    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 1rem;
        max-width: 96%;
    }

    section[data-testid="stSidebar"] {
        background: #f8fafc;
        border-right: 1px solid #e5e7eb;
    }

    .main-title {
        font-size: 38px;
        font-weight: 800;
        color: #111827;
        margin-bottom: 0.15rem;
    }

    .sub-title {
        font-size: 14px;
        color: #6b7280;
        margin-bottom: 1.2rem;
    }

    .card {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        padding: 18px 18px 14px 18px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        min-height: 112px;
    }

    .card-label {
        font-size: 13px;
        color: #6b7280;
        margin-bottom: 10px;
    }

    .card-value {
        font-size: 28px;
        font-weight: 800;
        color: #111827;
        line-height: 1.1;
    }

    .panel {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        padding: 18px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }

    .panel-title {
        font-size: 22px;
        font-weight: 700;
        color: #111827;
        margin-bottom: 12px;
    }

    .status-pill {
        display: inline-block;
        padding: 6px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 700;
        margin-right: 8px;
        margin-bottom: 8px;
    }

    .success-pill {
        background: #dcfce7;
        color: #166534;
    }

    .failed-pill {
        background: #fee2e2;
        color: #991b1b;
    }

    .notfound-pill {
        background: #fee2e2;
        color: #991b1b;
    }

    .warning-pill {
        background: #fef3c7;
        color: #92400e;
    }

    .neutral-pill {
        background: #e5e7eb;
        color: #374151;
    }

    .info-pill {
        background: #dbeafe;
        color: #1d4ed8;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        overflow: hidden;
    }

    .small-gap {
        height: 8px;
    }

    .meta-box {
        background: #f8fafc;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 10px;
    }

    .meta-label {
        font-size: 12px;
        color: #6b7280;
        margin-bottom: 4px;
    }

    .meta-value {
        font-size: 16px;
        font-weight: 700;
        color: #111827;
    }
</style>
""", unsafe_allow_html=True)

# =========================================================
# FIXED LOGIN
# =========================================================
APP_USERNAME = "net32team"
APP_PASSWORD = "Texas@teamnet32"
AUTH_TOKEN = hashlib.sha256(f"{APP_USERNAME}:{APP_PASSWORD}:net32-dashboard".encode()).hexdigest()

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "auth_checked" not in st.session_state:
    st.session_state.auth_checked = False


# =========================================================
# AUTH HELPERS
# =========================================================
def restore_auth():
    token = st.query_params.get("auth", "")
    if token == AUTH_TOKEN:
        st.session_state.authenticated = True


def set_auth():
    st.session_state.authenticated = True
    st.query_params["auth"] = AUTH_TOKEN


def clear_auth():
    st.session_state.authenticated = False
    if "auth" in st.query_params:
        del st.query_params["auth"]


def show_login():
    st.markdown('<div class="main-title">Net32 Inventory Sync Dashboard</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-title">Secure internal dashboard — actual sync runs from sync_inventory.py</div>',
        unsafe_allow_html=True
    )

    a, b, c = st.columns([1, 1.15, 1])
    with b:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.markdown('<div class="panel-title">Login</div>', unsafe_allow_html=True)

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login", use_container_width=True):
            if username.strip() == APP_USERNAME and password.strip() == APP_PASSWORD:
                set_auth()
                st.success("Login successful")
                st.rerun()
            else:
                st.error("Invalid username or password")

        st.markdown('</div>', unsafe_allow_html=True)


if not st.session_state.auth_checked:
    restore_auth()
    st.session_state.auth_checked = True

if not st.session_state.authenticated:
    show_login()
    st.stop()

# =========================================================
# IMPORT SYNC MODULE
# =========================================================
try:
    import sync_inventory as sync_module
except Exception as e:
    st.error(
        "Could not import sync_inventory.py.\n\n"
        "Make sure dashboard.py and sync_inventory.py are in the same folder.\n\n"
        f"Import error: {e}"
    )
    st.stop()

# =========================================================
# HELPERS
# =========================================================
def read_csv_if_exists(path):
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def read_text_if_exists(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return ""
    return ""


def read_json_if_exists(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def get_file_time(path):
    if os.path.exists(path):
        try:
            return pd.to_datetime(os.path.getmtime(path), unit="s").strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "N/A"
    return "N/A"


def make_card(label, value):
    st.markdown(
        f"""
        <div class="card">
            <div class="card-label">{label}</div>
            <div class="card-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def safe_int(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def make_status_pill(status):
    status = str(status).strip().lower()

    if status == "success":
        cls = "success-pill"
    elif status == "failed":
        cls = "failed-pill"
    elif status == "not_found_in_veeqo":
        cls = "notfound-pill"
    elif status == "unchanged":
        cls = "info-pill"
    elif status == "pending":
        cls = "warning-pill"
    else:
        cls = "neutral-pill"

    st.markdown(
        f'<span class="status-pill {cls}">{status}</span>',
        unsafe_allow_html=True
    )


def make_meta_box(label, value):
    st.markdown(
        f"""
        <div class="meta-box">
            <div class="meta-label">{label}</div>
            <div class="meta-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# =========================================================
# FILE PATHS FROM SYNC MODULE
# =========================================================
RESULTS_FILE = getattr(sync_module, "RESULTS_FILE", "sync_results.csv")
LOG_FILE = getattr(sync_module, "LOG_FILE", "sync_log.txt")
MAPPING_FILE = getattr(sync_module, "MAPPING_FILE", "sku_mpid_map.csv")
STATE_FILE = getattr(sync_module, "STATE_FILE", "last_sent_state.json")
SUMMARY_FILE = getattr(sync_module, "SUMMARY_FILE", "sync_summary.json")
TARGET_WAREHOUSE_NAME = getattr(sync_module, "TARGET_WAREHOUSE_NAME", "GP-WH")
QUANTITY_TYPE = "physical_stock_level"

# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:
    st.markdown("## Control Panel")

    st.markdown("### Account")
    if st.button("Logout", use_container_width=True):
        clear_auth()
        st.rerun()

    st.markdown("---")
    st.markdown("### Sync Actions")

    dry_run = st.toggle(
        "Dry Run",
        value=getattr(sync_module, "DRY_RUN", False),
        help="If enabled, dashboard will simulate Net32 update without sending live API request."
    )

    start_sync = st.button("Start Inventory Sync", type="primary", use_container_width=True)
    refresh_dashboard = st.button("Refresh Dashboard", use_container_width=True)

    st.markdown("---")
    st.markdown("### Sync Config")
    st.caption(f"Warehouse: {TARGET_WAREHOUSE_NAME}")
    st.caption(f"Quantity Type: {QUANTITY_TYPE}")

    st.markdown("---")
    st.markdown("### System Files")
    st.caption(f"Mapping: {MAPPING_FILE}")
    st.caption(f"Results: {RESULTS_FILE}")
    st.caption(f"Logs: {LOG_FILE}")
    st.caption(f"State: {STATE_FILE}")
    st.caption(f"Summary: {SUMMARY_FILE}")

    st.markdown("---")
    st.markdown("### Last Updated")
    st.caption(f"Results: {get_file_time(RESULTS_FILE)}")
    st.caption(f"Logs: {get_file_time(LOG_FILE)}")
    st.caption(f"Summary: {get_file_time(SUMMARY_FILE)}")

if refresh_dashboard:
    st.rerun()

sync_module.DRY_RUN = dry_run

# =========================================================
# RUN SYNC
# =========================================================
if start_sync:
    try:
        with st.spinner("Running inventory sync from sync_inventory.py ..."):
            sync_module.run_sync()
        st.success("Sync completed successfully.")
        st.rerun()
    except Exception as e:
        st.error(f"Sync failed: {e}")

# =========================================================
# LOAD DATA
# =========================================================
summary = read_json_if_exists(SUMMARY_FILE)
results_df = read_csv_if_exists(RESULTS_FILE)
mapping_df = read_csv_if_exists(MAPPING_FILE)
log_text = read_text_if_exists(LOG_FILE)
state_data = read_json_if_exists(STATE_FILE)

warehouse_used = summary.get("warehouse_used", TARGET_WAREHOUSE_NAME) if summary else TARGET_WAREHOUSE_NAME
quantity_type_used = summary.get("quantity_type", QUANTITY_TYPE) if summary else QUANTITY_TYPE

# =========================================================
# HEADER
# =========================================================
st.markdown('<div class="main-title">Net32 Inventory Sync Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">Dashboard connected with sync_inventory.py — sync uses GP-WH warehouse on-hand quantity</div>',
    unsafe_allow_html=True
)

# =========================================================
# META INFO
# =========================================================
m1, m2, m3, m4 = st.columns(4)
with m1:
    make_meta_box("Warehouse Used", warehouse_used)
with m2:
    make_meta_box("Quantity Type", quantity_type_used)
with m3:
    make_meta_box("Dry Run", "Yes" if dry_run else "No")
with m4:
    make_meta_box("Last Results Update", get_file_time(RESULTS_FILE))

st.markdown('<div class="small-gap"></div>', unsafe_allow_html=True)

# =========================================================
# SUMMARY CARDS
# =========================================================
st.markdown("## Summary")
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    make_card(
        "Total Mapping",
        safe_int(summary.get("total_mapping_rows", len(mapping_df)) if summary else len(mapping_df))
    )
with col2:
    make_card(
        "Found in Veeqo",
        safe_int(summary.get("found_in_veeqo", 0) if summary else 0)
    )
with col3:
    make_card(
        "Unchanged",
        safe_int(summary.get("unchanged", 0) if summary else 0)
    )
with col4:
    make_card(
        "Success",
        safe_int(summary.get("success", 0) if summary else 0)
    )
with col5:
    make_card(
        "Failed",
        safe_int(summary.get("failed", 0) if summary else 0)
    )
with col6:
    make_card(
        "Not Found",
        safe_int(summary.get("not_found_in_veeqo", 0) if summary else 0)
    )

st.markdown('<div class="small-gap"></div>', unsafe_allow_html=True)

# =========================================================
# OVERVIEW ROW
# =========================================================
left, right = st.columns([1.55, 1])

with left:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Status Breakdown</div>', unsafe_allow_html=True)

    if not results_df.empty and "status" in results_df.columns:
        status_counts = (
            results_df["status"]
            .fillna("unknown")
            .astype(str)
            .value_counts()
            .reset_index()
        )
        status_counts.columns = ["Status", "Count"]
        st.dataframe(status_counts, use_container_width=True, hide_index=True)
    else:
        st.info("No status data available yet.")

    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Quick Info</div>', unsafe_allow_html=True)

    st.markdown(f"**Mapping Rows:** {len(mapping_df) if not mapping_df.empty else 0}")
    st.markdown(f"**Result Rows:** {len(results_df) if not results_df.empty else 0}")
    st.markdown(f"**State Records:** {len(state_data) if isinstance(state_data, dict) else 0}")
    st.markdown(f"**Warehouse:** {warehouse_used}")
    st.markdown(f"**Quantity Type:** {quantity_type_used}")
    st.markdown(f"**Results Updated:** {get_file_time(RESULTS_FILE)}")
    st.markdown(f"**Logs Updated:** {get_file_time(LOG_FILE)}")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("**Active Statuses**")

    if not results_df.empty and "status" in results_df.columns:
        active_statuses = (
            results_df["status"]
            .fillna("unknown")
            .astype(str)
            .value_counts()
            .index
            .tolist()
        )
        for s in active_statuses:
            make_status_pill(s)
    else:
        st.caption("No status data yet.")

    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="small-gap"></div>', unsafe_allow_html=True)

# =========================================================
# TABS
# =========================================================
tab1, tab2, tab3, tab4 = st.tabs(["Results", "Mapping", "Logs", "State"])

with tab1:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Sync Results</div>', unsafe_allow_html=True)

    if not results_df.empty:
        filtered_df = results_df.copy()

        filter_col1, filter_col2 = st.columns([1, 1])

        with filter_col1:
            if "status" in filtered_df.columns:
                statuses = sorted(
                    filtered_df["status"].fillna("unknown").astype(str).unique().tolist()
                )
                selected_statuses = st.multiselect(
                    "Filter by Status",
                    options=statuses,
                    default=statuses
                )
                if selected_statuses:
                    filtered_df = filtered_df[
                        filtered_df["status"].fillna("unknown").astype(str).isin(selected_statuses)
                    ]
                else:
                    filtered_df = filtered_df.iloc[0:0]

        with filter_col2:
            search_text = st.text_input("Search in results", value="").strip().lower()
            if search_text:
                filtered_df = filtered_df[
                    filtered_df.astype(str).apply(
                        lambda row: row.str.lower().str.contains(search_text, na=False)
                    ).any(axis=1)
                ]

        st.dataframe(filtered_df, use_container_width=True, height=430)

        st.download_button(
            "Download Results CSV",
            data=filtered_df.to_csv(index=False).encode("utf-8"),
            file_name="sync_results.csv",
            mime="text/csv"
        )
    else:
        st.info("No results file found yet.")

    st.markdown('</div>', unsafe_allow_html=True)

with tab2:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Mapping Preview</div>', unsafe_allow_html=True)

    if not mapping_df.empty:
        st.caption(f"Total rows: {len(mapping_df)}")
        st.dataframe(mapping_df.head(300), use_container_width=True, height=430)

        st.download_button(
            "Download Mapping CSV",
            data=mapping_df.to_csv(index=False).encode("utf-8"),
            file_name=os.path.basename(MAPPING_FILE),
            mime="text/csv"
        )
    else:
        st.info("Mapping file not found yet.")

    st.markdown('</div>', unsafe_allow_html=True)

with tab3:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Logs</div>', unsafe_allow_html=True)

    if log_text.strip():
        st.text_area("sync_log.txt", value=log_text, height=430)

        st.download_button(
            "Download Log File",
            data=log_text.encode("utf-8"),
            file_name="sync_log.txt",
            mime="text/plain"
        )
    else:
        st.info("No log file found yet.")

    st.markdown('</div>', unsafe_allow_html=True)

with tab4:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Last Sent State</div>', unsafe_allow_html=True)

    if isinstance(state_data, dict) and state_data:
        state_rows = [{"sku": k, "last_sent_qty": v} for k, v in state_data.items()]
        state_df = pd.DataFrame(state_rows)

        search_state = st.text_input("Search in state", value="", key="state_search").strip().lower()
        if search_state:
            state_df = state_df[
                state_df.astype(str).apply(
                    lambda row: row.str.lower().str.contains(search_state, na=False)
                ).any(axis=1)
            ]

        st.dataframe(state_df, use_container_width=True, height=430)

        st.download_button(
            "Download State JSON",
            data=json.dumps(state_data, indent=2).encode("utf-8"),
            file_name="last_sent_state.json",
            mime="application/json"
        )
    else:
        st.info("No state data found yet.")

    st.markdown('</div>', unsafe_allow_html=True)