import streamlit as st
import pandas as pd
import gspread
import json
from google.oauth2.service_account import Credentials
from datetime import date, datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SHEET_NAMES = {
    "workers": "workers",
    "work_types": "work_types",
    "work_logs": "work_logs",
    "tools": "tools",
    "tool_moves": "tool_moves",
    "storage_places": "storage_places",
    "chekkulu": "chekkulu",
    "cold_storage": "cold_storage",
}

LABELS = {
    # Page names
    "dashboard": "డాష్\u200cబోర్డ్",
    "workers": "కూలీలు",
    "work_logs": "పని రికార్డులు",
    "tools": "పరికరాలు",
    "tool_moves": "పరికరాల తరలింపు",
    # Dashboard
    "active_workers": "పని చేస్తున్న కూలీలు",
    "total_tools": "మొత్తం పరికరాలు",
    "needs_repair": "మరమ్మత్తు అవసరం",
    "unpaid_amount": "చెల్లించని మొత్తం (₹)",
    "unpaid_work_logs": "చెల్లించని పని రికార్డులు",
    "repair_tools": "మరమ్మత్తు అవసరమైన పరికరాలు",
    # Workers
    "add_worker": "కొత్త కూలీని చేర్చు",
    "edit_worker": "కూలీ వివరాలు మార్చు",
    "worker_id": "కూలీ ID",
    "name": "పేరు",
    "phone": "ఫోన్",
    "daily_wage": "రోజు కూలి (₹)",
    "active": "పని చేస్తున్నారా",
    "notes": "నోట్స్",
    # Work logs
    "add_work_log": "కొత్త పని రికార్డు",
    "mark_payment": "చెల్లింపు నమోదు",
    "date": "తేదీ",
    "worker": "కూలీ",
    "work_type": "పని రకం",
    "day_unit": "రోజు / సగం రోజు",
    "rate": "రేటు (₹)",
    "amount_due": "చెల్లించాల్సిన మొత్తం (₹)",
    "pay_status": "చెల్లింపు స్థితి",
    "amount_paid": "చెల్లించిన మొత్తం (₹)",
    "pay_method": "చెల్లింపు విధానం",
    "filter_date": "తేదీ ఫిల్టర్",
    "filter_worker": "కూలీ ఫిల్టర్",
    "filter_pay_status": "చెల్లింపు స్థితి ఫిల్టర్",
    # Tools
    "tool_id": "పరికరం ID",
    "tool_name": "పరికరం పేరు",
    "tool_type": "రకం",
    "quantity": "సంఖ్య",
    "status": "స్థితి",
    "location": "ప్రస్తుత స్థలం",
    "last_updated": "చివరి అప్డేట్",
    "update_status": "స్థితి మార్చు",
    "filter_type": "రకం ఫిల్టర్",
    "filter_status": "స్థితి ఫిల్టర్",
    # Tool moves
    "add_move": "కొత్త తరలింపు",
    "move_id": "తరలింపు ID",
    "tool": "పరికరం",
    "from_place": "ఎక్కడ నుండి",
    "to_place": "ఎక్కడికి",
    "moved_by": "తరలించినవారు",
    "movement_history": "తరలింపు చరిత్ర",
    # Chekkulu (Tobacco Bales)
    "chekkulu": "చెక్కులు",
    "add_chekkulu": "కొత్త చెక్క చేర్చు",
    "chekkulu_id": "చెక్క ID",
    "chekkulu_rate": "రేటు",
    "chekkulu_weight": "బరువు",
    "tbgr_number": "TBGR నంబర్",
    "chekkulu_type": "రకం",
    # Cold Storage
    "cold_storage": "కోల్డ్ స్టోరేజ్",
    "add_cold_storage": "కొత్త ఐటమ్ చేర్చు",
    "cold_storage_id": "ID",
    "date_stored": "దాచిన తేదీ",
    "count": "సంఖ్య",
    "weight": "బరువు",
    "serial_number": "సీరియల్ నంబర్",
    "date_removed": "తీసిన తేదీ",
    "mark_removed": "తీసినట్టు నమోదు",
    # Filters – Chekkulu
    "filter_ck_date": "తేదీ ఫిల్టర్",
    "filter_tbgr": "TBGR ఫిల్టర్",
    "filter_ck_type": "రకం ఫిల్టర్",
    # Filters – Cold Storage
    "filter_cs_date_stored": "దాచిన తేదీ ఫిల్టర్",
    "filter_cs_date_removed": "తీసిన తేదీ ఫిల్టర్",
    "filter_serial": "సీరియల్ నంబర్ ఫిల్టర్",
    "filter_cs_type": "రకం ఫిల్టర్",
    "cs_type": "రకం",
    # Common
    "save": "సేవ్ చేయి",
    "submit": "సమర్పించు",
    "all": "అన్నీ",
    "yes": "అవును",
    "no": "కాదు",
    "full_day": "పూర్తి రోజు",
    "half_day": "సగం రోజు",
}

TOOL_STATUSES = ["బాగుంది", "మరమ్మత్తు అవసరం", "పనిచేయడం లేదు"]
PAY_METHODS = ["నగదు", "UPI"]
PAY_STATUSES = ["PAID", "PARTIAL", "UNPAID"]

# ---------------------------------------------------------------------------
# Google Sheets connection
# ---------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


@st.cache_resource
def get_gspread_client():
    creds_dict = st.secrets["gcp_service_account"]
    # st.secrets returns an AttrDict; convert to plain dict for google-auth
    creds_dict = dict(creds_dict)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(st.secrets["spreadsheet_id"])


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

SHEET_HEADERS = {
    "chekkulu": ["chekkulu_id", "date", "rate", "weight", "tbgr_number", "type"],
    "cold_storage": ["cold_storage_id", "date_stored", "count", "weight",
                     "serial_number", "type", "date_removed"],
}


def ensure_worksheet(sheet_name: str):
    """Create the worksheet with headers if it does not exist yet."""
    ss = get_spreadsheet()
    try:
        ss.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_name, rows=1000, cols=20)
        if sheet_name in SHEET_HEADERS:
            ws.update(range_name="A1", values=[SHEET_HEADERS[sheet_name]])


def load_sheet(sheet_name: str) -> pd.DataFrame:
    ss = get_spreadsheet()
    ensure_worksheet(sheet_name)
    ws = ss.worksheet(sheet_name)
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    if df.empty:
        # Return an empty DataFrame with the header row as columns
        header = ws.row_values(1)
        df = pd.DataFrame(columns=header)
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].fillna("")
    return df


def save_sheet(df: pd.DataFrame, sheet_name: str):
    ss = get_spreadsheet()
    ws = ss.worksheet(sheet_name)
    ws.clear()
    # Build list-of-lists: header + rows
    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    ws.update(range_name="A1", values=data)


def get_data(key: str, sheet_name: str) -> pd.DataFrame:
    if key not in st.session_state:
        st.session_state[key] = load_sheet(sheet_name)
    return st.session_state[key]


def refresh(key: str, sheet_name: str):
    st.session_state[key] = load_sheet(sheet_name)


def next_id(df: pd.DataFrame, id_col: str, prefix: str, width: int) -> str:
    if df.empty:
        return f"{prefix}{1:0{width}d}"
    nums = df[id_col].str.replace(prefix, "", regex=False).astype(int)
    return f"{prefix}{nums.max() + 1:0{width}d}"


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

st.set_page_config(page_title="వ్యవసాయ నిర్వహణ", page_icon="🌾", layout="wide")

# --- Password gate ---
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    st.title("🔒 లాగిన్")
    password = st.text_input("పాస్‌వర్డ్ ఎంటర్ చేయండి", type="password")
    if password:
        if password == st.secrets["app_password"]:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("పాస్‌వర్డ్ తప్పు. మళ్ళీ ప్రయత్నించండి.")
    st.stop()

st.title("🌾 వ్యవసాయ నిర్వహణ")

page = st.sidebar.radio(
    "మెనూ",
    [
        LABELS["dashboard"],
        LABELS["workers"],
        LABELS["work_logs"],
        LABELS["tools"],
        LABELS["tool_moves"],
        LABELS["chekkulu"],
        LABELS["cold_storage"],
    ],
)

# ---------------------------------------------------------------------------
# PAGE: Dashboard
# ---------------------------------------------------------------------------
if page == LABELS["dashboard"]:
    workers = get_data("workers", SHEET_NAMES["workers"])
    tools = get_data("tools", SHEET_NAMES["tools"])
    work_logs = get_data("work_logs", SHEET_NAMES["work_logs"])

    active_count = int((workers["active"] == "Y").sum())
    total_tools = len(tools)
    repair_count = int((tools["status_te"] != "బాగుంది").sum())

    unpaid_logs = work_logs[work_logs["pay_status"].isin(["UNPAID", "PARTIAL"])].copy()
    unpaid_total = float((unpaid_logs["amount_due"] - unpaid_logs["amount_paid"]).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(LABELS["active_workers"], active_count)
    c2.metric(LABELS["total_tools"], total_tools)
    c3.metric(LABELS["needs_repair"], repair_count)
    c4.metric(LABELS["unpaid_amount"], f"₹{unpaid_total:,.0f}")

    st.subheader(LABELS["unpaid_work_logs"])
    if unpaid_logs.empty:
        st.info("అన్ని చెల్లింపులు పూర్తయ్యాయి!")
    else:
        display = unpaid_logs[["work_log_id", "date", "worker_name_te", "work_type_te",
                                "amount_due", "amount_paid", "pay_status"]].copy()
        display.columns = [LABELS["worker_id"], LABELS["date"], LABELS["name"],
                           LABELS["work_type"], LABELS["amount_due"],
                           LABELS["amount_paid"], LABELS["pay_status"]]
        st.dataframe(display, hide_index=True, use_container_width=True)

    st.subheader(LABELS["repair_tools"])
    repair_tools = tools[tools["status_te"] != "బాగుంది"].copy()
    if repair_tools.empty:
        st.info("అన్ని పరికరాలు బాగున్నాయి!")
    else:
        display_t = repair_tools[["tool_id", "name_te", "tool_type", "status_te",
                                   "current_place_te"]].copy()
        display_t.columns = [LABELS["tool_id"], LABELS["tool_name"], LABELS["tool_type"],
                             LABELS["status"], LABELS["location"]]
        st.dataframe(display_t, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# PAGE: Workers
# ---------------------------------------------------------------------------
elif page == LABELS["workers"]:
    workers = get_data("workers", SHEET_NAMES["workers"])

    st.subheader(LABELS["workers"])
    display_w = workers[["worker_id", "name_te", "phone", "default_daily_wage",
                          "active", "notes"]].copy()
    display_w.columns = [LABELS["worker_id"], LABELS["name"], LABELS["phone"],
                         LABELS["daily_wage"], LABELS["active"], LABELS["notes"]]
    st.dataframe(display_w, hide_index=True, use_container_width=True)

    # --- Add worker ---
    with st.expander(LABELS["add_worker"], expanded=False):
        with st.form("add_worker_form"):
            new_name = st.text_input(LABELS["name"])
            new_phone = st.text_input(LABELS["phone"])
            new_wage = st.number_input(LABELS["daily_wage"], min_value=0, value=550, step=50)
            new_active = st.selectbox(LABELS["active"], ["Y", "N"])
            new_notes = st.text_input(LABELS["notes"])
            submitted = st.form_submit_button(LABELS["save"])

        if submitted and new_name.strip():
            new_id = next_id(workers, "worker_id", "W", 3)
            new_row = pd.DataFrame([{
                "worker_id": new_id,
                "name_te": new_name.strip(),
                "phone": new_phone.strip(),
                "default_daily_wage": int(new_wage),
                "active": new_active,
                "notes": new_notes.strip(),
            }])
            workers = pd.concat([workers, new_row], ignore_index=True)
            save_sheet(workers, SHEET_NAMES["workers"])
            st.session_state["workers"] = workers
            st.success(f"కూలీ {new_id} చేర్చబడింది!")
            st.rerun()

    # --- Edit worker ---
    with st.expander(LABELS["edit_worker"], expanded=False):
        worker_options = [
            f"{r.name_te} ({r.worker_id})" for _, r in workers.iterrows()
        ]
        if worker_options:
            sel = st.selectbox("కూలీని ఎంచుకోండి", worker_options, key="edit_worker_sel")
            sel_id = sel.split("(")[-1].rstrip(")")
            row = workers[workers["worker_id"] == sel_id].iloc[0]

            with st.form("edit_worker_form"):
                ed_name = st.text_input(LABELS["name"], value=row["name_te"])
                ed_phone = st.text_input(LABELS["phone"], value=str(row["phone"]))
                ed_wage = st.number_input(LABELS["daily_wage"], min_value=0,
                                          value=int(row["default_daily_wage"]), step=50)
                ed_active = st.selectbox(LABELS["active"], ["Y", "N"],
                                         index=0 if row["active"] == "Y" else 1)
                ed_notes = st.text_input(LABELS["notes"], value=str(row["notes"]))
                ed_submit = st.form_submit_button(LABELS["save"])

            if ed_submit:
                idx = workers.index[workers["worker_id"] == sel_id][0]
                workers.at[idx, "name_te"] = ed_name.strip()
                workers.at[idx, "phone"] = ed_phone.strip()
                workers.at[idx, "default_daily_wage"] = int(ed_wage)
                workers.at[idx, "active"] = ed_active
                workers.at[idx, "notes"] = ed_notes.strip()
                save_sheet(workers, SHEET_NAMES["workers"])
                st.session_state["workers"] = workers
                st.success(f"కూలీ {sel_id} అప్డేట్ చేయబడింది!")
                st.rerun()

# ---------------------------------------------------------------------------
# PAGE: Work Logs
# ---------------------------------------------------------------------------
elif page == LABELS["work_logs"]:
    work_logs = get_data("work_logs", SHEET_NAMES["work_logs"])
    workers = get_data("workers", SHEET_NAMES["workers"])
    work_types = get_data("work_types", SHEET_NAMES["work_types"])

    st.subheader(LABELS["work_logs"])

    # --- Filters ---
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        dates_in_data = sorted(work_logs["date"].unique())
        min_d = datetime.strptime(dates_in_data[0], "%Y-%m-%d").date() if dates_in_data else date.today()
        max_d = datetime.strptime(dates_in_data[-1], "%Y-%m-%d").date() if dates_in_data else date.today()
        date_range = st.date_input(LABELS["filter_date"], value=(min_d, max_d),
                                   min_value=min_d, max_value=max_d, key="wl_date_range")
    with fc2:
        worker_names = [LABELS["all"]] + sorted(work_logs["worker_name_te"].unique().tolist())
        sel_worker = st.selectbox(LABELS["filter_worker"], worker_names, key="wl_worker_filter")
    with fc3:
        status_opts = [LABELS["all"]] + PAY_STATUSES
        sel_status = st.selectbox(LABELS["filter_pay_status"], status_opts, key="wl_status_filter")

    filtered = work_logs.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        d_start, d_end = date_range
        filtered = filtered[
            (filtered["date"] >= d_start.strftime("%Y-%m-%d")) &
            (filtered["date"] <= d_end.strftime("%Y-%m-%d"))
        ]
    if sel_worker != LABELS["all"]:
        filtered = filtered[filtered["worker_name_te"] == sel_worker]
    if sel_status != LABELS["all"]:
        filtered = filtered[filtered["pay_status"] == sel_status]

    display_wl = filtered[["work_log_id", "date", "worker_name_te", "work_type_te",
                            "day_unit", "rate_daily", "amount_due", "amount_paid",
                            "pay_status", "pay_method", "notes"]].copy()
    display_wl.columns = ["ID", LABELS["date"], LABELS["name"], LABELS["work_type"],
                          LABELS["day_unit"], LABELS["rate"], LABELS["amount_due"],
                          LABELS["amount_paid"], LABELS["pay_status"],
                          LABELS["pay_method"], LABELS["notes"]]
    st.dataframe(display_wl, hide_index=True, use_container_width=True)

    # --- Add work log ---
    with st.expander(LABELS["add_work_log"], expanded=False):
        active_workers = workers[workers["active"] == "Y"].copy()
        worker_opts = [
            f"{r.name_te} ({r.worker_id})" for _, r in active_workers.iterrows()
        ]
        wt_opts = [
            f"{r.name_te} ({r.work_type_id})" for _, r in work_types.iterrows()
        ]

        with st.form("add_wl_form"):
            wl_date = st.date_input(LABELS["date"], value=date.today())
            wl_worker = st.selectbox(LABELS["worker"], worker_opts)
            wl_type = st.selectbox(LABELS["work_type"], wt_opts)
            wl_unit = st.selectbox(LABELS["day_unit"], ["FULL", "HALF"],
                                   format_func=lambda x: LABELS["full_day"] if x == "FULL"
                                   else LABELS["half_day"])

            # Extract IDs
            w_id = wl_worker.split("(")[-1].rstrip(")")
            wt_id = wl_type.split("(")[-1].rstrip(")")
            w_row = active_workers[active_workers["worker_id"] == w_id].iloc[0]
            wt_row = work_types[work_types["work_type_id"] == wt_id].iloc[0]

            wl_rate = st.number_input(LABELS["rate"], min_value=0,
                                      value=int(w_row["default_daily_wage"]), step=50)
            wl_amount_paid = st.number_input(LABELS["amount_paid"], min_value=0, value=0, step=50)
            wl_pay_method = st.selectbox(LABELS["pay_method"], [""] + PAY_METHODS)
            wl_notes = st.text_input(LABELS["notes"])
            wl_submit = st.form_submit_button(LABELS["submit"])

        if wl_submit:
            amount_due = int(wl_rate) if wl_unit == "FULL" else int(wl_rate) // 2
            paid = int(wl_amount_paid)
            if paid == 0:
                pay_st = "UNPAID"
            elif paid >= amount_due:
                pay_st = "PAID"
            else:
                pay_st = "PARTIAL"

            new_wl_id = next_id(work_logs, "work_log_id", "WL", 6)
            new_wl = pd.DataFrame([{
                "work_log_id": new_wl_id,
                "date": wl_date.strftime("%Y-%m-%d"),
                "worker_id": w_id,
                "worker_name_te": w_row["name_te"],
                "work_type_id": wt_id,
                "work_type_te": wt_row["name_te"],
                "day_unit": wl_unit,
                "rate_daily": int(wl_rate),
                "amount_due": amount_due,
                "pay_status": pay_st,
                "amount_paid": paid,
                "pay_method": wl_pay_method if paid > 0 else "",
                "notes": wl_notes.strip(),
            }])
            work_logs = pd.concat([work_logs, new_wl], ignore_index=True)
            save_sheet(work_logs, SHEET_NAMES["work_logs"])
            st.session_state["work_logs"] = work_logs
            st.success(f"పని రికార్డు {new_wl_id} చేర్చబడింది!")
            st.rerun()

    # --- Mark payment ---
    with st.expander(LABELS["mark_payment"], expanded=False):
        unpaid = work_logs[work_logs["pay_status"].isin(["UNPAID", "PARTIAL"])].copy()
        if unpaid.empty:
            st.info("చెల్లించని రికార్డులు లేవు!")
        else:
            pay_opts = [
                f"{r.work_log_id} | {r.date} | {r.worker_name_te} | ₹{r.amount_due} (చెల్లించింది: ₹{r.amount_paid})"
                for _, r in unpaid.iterrows()
            ]
            with st.form("mark_pay_form"):
                sel_pay = st.selectbox("రికార్డు ఎంచుకోండి", pay_opts)
                sel_pay_id = sel_pay.split(" | ")[0]
                sel_row = work_logs[work_logs["work_log_id"] == sel_pay_id].iloc[0]
                remaining = int(sel_row["amount_due"]) - int(sel_row["amount_paid"])

                pay_amount = st.number_input(
                    f"చెల్లించే మొత్తం (బాకీ: ₹{remaining})",
                    min_value=0, max_value=remaining, value=remaining, step=50
                )
                pay_method = st.selectbox(LABELS["pay_method"], PAY_METHODS, key="pay_method_mark")
                pay_submit = st.form_submit_button(LABELS["submit"])

            if pay_submit and pay_amount > 0:
                idx = work_logs.index[work_logs["work_log_id"] == sel_pay_id][0]
                new_paid = int(work_logs.at[idx, "amount_paid"]) + pay_amount
                work_logs.at[idx, "amount_paid"] = new_paid
                due = int(work_logs.at[idx, "amount_due"])
                if new_paid >= due:
                    work_logs.at[idx, "pay_status"] = "PAID"
                elif new_paid > 0:
                    work_logs.at[idx, "pay_status"] = "PARTIAL"
                # Update pay method
                work_logs.at[idx, "pay_method"] = pay_method
                save_sheet(work_logs, SHEET_NAMES["work_logs"])
                st.session_state["work_logs"] = work_logs
                st.success(f"₹{pay_amount} చెల్లింపు నమోదు చేయబడింది!")
                st.rerun()

# ---------------------------------------------------------------------------
# PAGE: Tools
# ---------------------------------------------------------------------------
elif page == LABELS["tools"]:
    tools = get_data("tools", SHEET_NAMES["tools"])

    st.subheader(LABELS["tools"])

    # --- Filters ---
    fc1, fc2 = st.columns(2)
    with fc1:
        type_opts = [LABELS["all"]] + sorted(tools["tool_type"].unique().tolist())
        sel_type = st.selectbox(LABELS["filter_type"], type_opts, key="tool_type_filter")
    with fc2:
        status_opts = [LABELS["all"]] + sorted(tools["status_te"].unique().tolist())
        sel_st = st.selectbox(LABELS["filter_status"], status_opts, key="tool_status_filter")

    filtered_tools = tools.copy()
    if sel_type != LABELS["all"]:
        filtered_tools = filtered_tools[filtered_tools["tool_type"] == sel_type]
    if sel_st != LABELS["all"]:
        filtered_tools = filtered_tools[filtered_tools["status_te"] == sel_st]

    display_tools = filtered_tools[["tool_id", "name_te", "tool_type", "quantity",
                                     "status_te", "current_place_te", "last_updated",
                                     "notes"]].copy()
    display_tools.columns = [LABELS["tool_id"], LABELS["tool_name"], LABELS["tool_type"],
                             LABELS["quantity"], LABELS["status"], LABELS["location"],
                             LABELS["last_updated"], LABELS["notes"]]
    st.dataframe(display_tools, hide_index=True, use_container_width=True)

    # --- Update status ---
    with st.expander(LABELS["update_status"], expanded=False):
        tool_opts = [
            f"{r.name_te} ({r.tool_id}) - {r.status_te}" for _, r in tools.iterrows()
        ]
        with st.form("update_status_form"):
            sel_tool = st.selectbox(LABELS["tool"], tool_opts)
            sel_tool_id = sel_tool.split("(")[1].split(")")[0]
            new_status = st.selectbox(LABELS["status"], TOOL_STATUSES)
            status_submit = st.form_submit_button(LABELS["save"])

        if status_submit:
            idx = tools.index[tools["tool_id"] == sel_tool_id][0]
            tools.at[idx, "status_te"] = new_status
            tools.at[idx, "last_updated"] = date.today().strftime("%Y-%m-%d")
            save_sheet(tools, SHEET_NAMES["tools"])
            st.session_state["tools"] = tools
            st.success(f"పరికరం {sel_tool_id} స్థితి '{new_status}' కి మార్చబడింది!")
            st.rerun()

# ---------------------------------------------------------------------------
# PAGE: Tool Moves
# ---------------------------------------------------------------------------
elif page == LABELS["tool_moves"]:
    tools = get_data("tools", SHEET_NAMES["tools"])
    tool_moves = get_data("tool_moves", SHEET_NAMES["tool_moves"])
    places = get_data("storage_places", SHEET_NAMES["storage_places"])

    st.subheader(LABELS["add_move"])

    tool_opts = [
        f"{r.name_te} ({r.tool_id}) - {r.current_place_te}" for _, r in tools.iterrows()
    ]
    place_opts = [
        f"{r.name_te} ({r.place_id})" for _, r in places.iterrows()
    ]

    with st.form("add_move_form"):
        mv_date = st.date_input(LABELS["date"], value=date.today())
        mv_tool = st.selectbox(LABELS["tool"], tool_opts)
        mv_tool_id = mv_tool.split("(")[1].split(")")[0]
        tool_row = tools[tools["tool_id"] == mv_tool_id].iloc[0]

        st.text_input(LABELS["from_place"], value=tool_row["current_place_te"], disabled=True)

        mv_to = st.selectbox(LABELS["to_place"], place_opts)
        mv_to_id = mv_to.split("(")[-1].rstrip(")")
        mv_to_name = places[places["place_id"] == mv_to_id].iloc[0]["name_te"]

        mv_by = st.text_input(LABELS["moved_by"])
        mv_notes = st.text_input(LABELS["notes"])
        mv_submit = st.form_submit_button(LABELS["submit"])

    if mv_submit:
        if mv_to_id == tool_row["current_place_id"]:
            st.error("పరికరం ఇప్పటికే ఆ స్థలంలో ఉంది! వేరే స్థలాన్ని ఎంచుకోండి.")
        else:
            new_mv_id = next_id(tool_moves, "tool_move_id", "TM", 6)
            new_move = pd.DataFrame([{
                "tool_move_id": new_mv_id,
                "date": mv_date.strftime("%Y-%m-%d"),
                "tool_id": mv_tool_id,
                "tool_name_te": tool_row["name_te"],
                "from_place_id": tool_row["current_place_id"],
                "from_place_te": tool_row["current_place_te"],
                "to_place_id": mv_to_id,
                "to_place_te": mv_to_name,
                "moved_by": mv_by.strip(),
                "notes": mv_notes.strip(),
            }])
            tool_moves = pd.concat([tool_moves, new_move], ignore_index=True)
            save_sheet(tool_moves, SHEET_NAMES["tool_moves"])
            st.session_state["tool_moves"] = tool_moves

            # Update tool's current location
            t_idx = tools.index[tools["tool_id"] == mv_tool_id][0]
            tools.at[t_idx, "current_place_id"] = mv_to_id
            tools.at[t_idx, "current_place_te"] = mv_to_name
            tools.at[t_idx, "last_updated"] = mv_date.strftime("%Y-%m-%d")
            save_sheet(tools, SHEET_NAMES["tools"])
            st.session_state["tools"] = tools

            st.success(f"పరికరం {mv_tool_id} తరలింపు {new_mv_id} నమోదు చేయబడింది!")
            st.rerun()

    st.subheader(LABELS["movement_history"])
    display_mv = tool_moves[["tool_move_id", "date", "tool_name_te", "from_place_te",
                              "to_place_te", "moved_by", "notes"]].copy()
    display_mv.columns = [LABELS["move_id"], LABELS["date"], LABELS["tool_name"],
                          LABELS["from_place"], LABELS["to_place"],
                          LABELS["moved_by"], LABELS["notes"]]
    # Show most recent first
    display_mv = display_mv.iloc[::-1].reset_index(drop=True)
    st.dataframe(display_mv, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# PAGE: చెక్కులు (Tobacco Bales)
# ---------------------------------------------------------------------------
elif page == LABELS["chekkulu"]:
    chekkulu = get_data("chekkulu", SHEET_NAMES["chekkulu"])

    st.subheader(LABELS["chekkulu"])

    if not chekkulu.empty:
        # --- Filters ---
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            ck_dates = sorted(chekkulu["date"].unique())
            ck_min_d = datetime.strptime(ck_dates[0], "%Y-%m-%d").date() if ck_dates else date.today()
            ck_max_d = datetime.strptime(ck_dates[-1], "%Y-%m-%d").date() if ck_dates else date.today()
            ck_date_range = st.date_input(LABELS["filter_ck_date"],
                                          value=(ck_min_d, ck_max_d),
                                          min_value=ck_min_d, max_value=ck_max_d,
                                          key="ck_date_filter")
        with fc2:
            tbgr_opts = [LABELS["all"]] + sorted(chekkulu["tbgr_number"].unique().tolist())
            sel_tbgr = st.selectbox(LABELS["filter_tbgr"], tbgr_opts, key="ck_tbgr_filter")
        with fc3:
            type_opts = [LABELS["all"]] + sorted(chekkulu["type"].unique().tolist())
            sel_ck_type = st.selectbox(LABELS["filter_ck_type"], type_opts, key="ck_type_filter")

        filtered_ck = chekkulu.copy()
        if isinstance(ck_date_range, tuple) and len(ck_date_range) == 2:
            d_start, d_end = ck_date_range
            filtered_ck = filtered_ck[
                (filtered_ck["date"] >= d_start.strftime("%Y-%m-%d")) &
                (filtered_ck["date"] <= d_end.strftime("%Y-%m-%d"))
            ]
        if sel_tbgr != LABELS["all"]:
            filtered_ck = filtered_ck[filtered_ck["tbgr_number"] == sel_tbgr]
        if sel_ck_type != LABELS["all"]:
            filtered_ck = filtered_ck[filtered_ck["type"] == sel_ck_type]

        display_ck = filtered_ck[["chekkulu_id", "date", "rate", "weight",
                                   "tbgr_number", "type"]].copy()
        display_ck.columns = [LABELS["chekkulu_id"], LABELS["date"],
                              LABELS["chekkulu_rate"], LABELS["chekkulu_weight"],
                              LABELS["tbgr_number"], LABELS["chekkulu_type"]]
        st.dataframe(display_ck, hide_index=True, use_container_width=True)
    else:
        st.info("చెక్కులు రికార్డులు లేవు.")

    # --- Add chekkulu ---
    with st.expander(LABELS["add_chekkulu"], expanded=False):
        with st.form("add_chekkulu_form"):
            ck_date = st.date_input(LABELS["date"], value=date.today())
            ck_rate = st.number_input(LABELS["chekkulu_rate"], min_value=0.0,
                                      value=0.0, step=0.5, format="%.2f")
            ck_weight = st.number_input(LABELS["chekkulu_weight"], min_value=0.0,
                                        value=0.0, step=0.5, format="%.2f")
            ck_tbgr = st.text_input(LABELS["tbgr_number"])
            ck_type = st.text_input(LABELS["chekkulu_type"])
            ck_submit = st.form_submit_button(LABELS["save"])

        if ck_submit:
            new_ck_id = next_id(chekkulu, "chekkulu_id", "CK", 6)
            new_ck = pd.DataFrame([{
                "chekkulu_id": new_ck_id,
                "date": ck_date.strftime("%Y-%m-%d"),
                "rate": ck_rate,
                "weight": ck_weight,
                "tbgr_number": ck_tbgr.strip(),
                "type": ck_type.strip(),
            }])
            chekkulu = pd.concat([chekkulu, new_ck], ignore_index=True)
            save_sheet(chekkulu, SHEET_NAMES["chekkulu"])
            st.session_state["chekkulu"] = chekkulu
            st.success(f"చెక్క {new_ck_id} చేర్చబడింది!")
            st.rerun()

# ---------------------------------------------------------------------------
# PAGE: కోల్డ్ స్టోరేజ్ (Cold Storage)
# ---------------------------------------------------------------------------
elif page == LABELS["cold_storage"]:
    cold_storage = get_data("cold_storage", SHEET_NAMES["cold_storage"])

    st.subheader(LABELS["cold_storage"])

    if not cold_storage.empty:
        # --- Filters ---
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            cs_dates = sorted([d for d in cold_storage["date_stored"].unique() if d])
            cs_min_d = datetime.strptime(cs_dates[0], "%Y-%m-%d").date() if cs_dates else date.today()
            cs_max_d = datetime.strptime(cs_dates[-1], "%Y-%m-%d").date() if cs_dates else date.today()
            cs_date_range = st.date_input(LABELS["filter_cs_date_stored"],
                                          value=(cs_min_d, cs_max_d),
                                          min_value=cs_min_d, max_value=cs_max_d,
                                          key="cs_date_stored_filter")
        with fc2:
            rm_dates = sorted([d for d in cold_storage["date_removed"].unique() if d])
            if rm_dates:
                rm_min_d = datetime.strptime(rm_dates[0], "%Y-%m-%d").date()
                rm_max_d = datetime.strptime(rm_dates[-1], "%Y-%m-%d").date()
                cs_rm_date_range = st.date_input(LABELS["filter_cs_date_removed"],
                                                 value=(rm_min_d, rm_max_d),
                                                 min_value=rm_min_d, max_value=rm_max_d,
                                                 key="cs_date_removed_filter")
            else:
                cs_rm_date_range = None
                st.text_input(LABELS["filter_cs_date_removed"], value="—", disabled=True)
        with fc3:
            serial_opts = [LABELS["all"]] + sorted([s for s in cold_storage["serial_number"].unique() if s])
            sel_serial = st.selectbox(LABELS["filter_serial"], serial_opts, key="cs_serial_filter")
        with fc4:
            cs_type_opts = [LABELS["all"]] + sorted([t for t in cold_storage["type"].unique() if t])
            sel_cs_type = st.selectbox(LABELS["filter_cs_type"], cs_type_opts, key="cs_type_filter")

        filtered_cs = cold_storage.copy()
        if isinstance(cs_date_range, tuple) and len(cs_date_range) == 2:
            d_start, d_end = cs_date_range
            filtered_cs = filtered_cs[
                (filtered_cs["date_stored"] >= d_start.strftime("%Y-%m-%d")) &
                (filtered_cs["date_stored"] <= d_end.strftime("%Y-%m-%d"))
            ]
        if cs_rm_date_range is not None and isinstance(cs_rm_date_range, tuple) and len(cs_rm_date_range) == 2:
            r_start, r_end = cs_rm_date_range
            filtered_cs = filtered_cs[
                (filtered_cs["date_removed"] >= r_start.strftime("%Y-%m-%d")) &
                (filtered_cs["date_removed"] <= r_end.strftime("%Y-%m-%d"))
            ]
        if sel_serial != LABELS["all"]:
            filtered_cs = filtered_cs[filtered_cs["serial_number"] == sel_serial]
        if sel_cs_type != LABELS["all"]:
            filtered_cs = filtered_cs[filtered_cs["type"] == sel_cs_type]

        display_cs = filtered_cs[["cold_storage_id", "date_stored", "count",
                                   "weight", "serial_number", "type",
                                   "date_removed"]].copy()
        # Build serial_number display: serial / count of rows with same serial
        serial_counts = cold_storage["serial_number"].value_counts()
        display_cs["serial_number"] = display_cs["serial_number"].apply(
            lambda s: f"{s} / {serial_counts[s]}" if s else ""
        )
        display_cs.columns = [LABELS["cold_storage_id"], LABELS["date_stored"],
                              LABELS["count"], LABELS["weight"],
                              LABELS["serial_number"], LABELS["cs_type"],
                              LABELS["date_removed"]]
        st.dataframe(display_cs, hide_index=True, use_container_width=True)
    else:
        st.info("కోల్డ్ స్టోరేజ్ రికార్డులు లేవు.")

    # --- Add cold storage item ---
    with st.expander(LABELS["add_cold_storage"], expanded=False):
        with st.form("add_cs_form"):
            cs_date = st.date_input(LABELS["date_stored"], value=date.today())
            cs_count = st.number_input(LABELS["count"], min_value=0, value=0, step=1)
            cs_weight = st.number_input(LABELS["weight"], min_value=0.0,
                                        value=0.0, step=0.5, format="%.2f")
            cs_serial = st.text_input(LABELS["serial_number"])
            cs_type = st.text_input(LABELS["cs_type"])
            cs_submit = st.form_submit_button(LABELS["save"])

        if cs_submit:
            new_cs_id = next_id(cold_storage, "cold_storage_id", "CS", 6)
            new_cs = pd.DataFrame([{
                "cold_storage_id": new_cs_id,
                "date_stored": cs_date.strftime("%Y-%m-%d"),
                "count": cs_count,
                "weight": cs_weight,
                "serial_number": cs_serial.strip(),
                "type": cs_type.strip(),
                "date_removed": "",
            }])
            cold_storage = pd.concat([cold_storage, new_cs], ignore_index=True)
            save_sheet(cold_storage, SHEET_NAMES["cold_storage"])
            st.session_state["cold_storage"] = cold_storage
            st.success(f"ఐటమ్ {new_cs_id} చేర్చబడింది!")
            st.rerun()

    # --- Mark as removed ---
    with st.expander(LABELS["mark_removed"], expanded=False):
        active_items = cold_storage[cold_storage["date_removed"] == ""].copy()
        if active_items.empty:
            st.info("తీయవలసిన ఐటమ్‌లు లేవు.")
        else:
            item_opts = [
                f"{r.cold_storage_id} | {r.serial_number} | బరువు: {r.weight}"
                for _, r in active_items.iterrows()
            ]
            with st.form("mark_removed_form"):
                sel_item = st.selectbox("ఐటమ్ ఎంచుకోండి", item_opts)
                sel_cs_id = sel_item.split(" | ")[0]
                rm_date = st.date_input(LABELS["date_removed"], value=date.today())
                rm_submit = st.form_submit_button(LABELS["save"])

            if rm_submit:
                idx = cold_storage.index[cold_storage["cold_storage_id"] == sel_cs_id][0]
                cold_storage.at[idx, "date_removed"] = rm_date.strftime("%Y-%m-%d")
                save_sheet(cold_storage, SHEET_NAMES["cold_storage"])
                st.session_state["cold_storage"] = cold_storage
                st.success(f"ఐటమ్ {sel_cs_id} తీసినట్టు నమోదు చేయబడింది!")
                st.rerun()
