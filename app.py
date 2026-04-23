"""
app.py - Streamlit UI for LGD Fuzzy Matcher
Run: streamlit run app.py
"""
import io, os, time
import json
import hmac
import base64
import hashlib
import uuid
from collections.abc import Mapping
import pandas as pd
import streamlit as st
from matcher import LGDMatcher
from utils import generate_sql_update, load_config

st.set_page_config(page_title="LGD Fuzzy Matcher", page_icon="🗺️", layout="wide")

def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # region agent log
    payload = {
        "sessionId": "c95d1a",
        "runId": f"pre-fix-{st.session_state.get('debug_run_id', 'unknown')}",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    # Keep this tiny and non-blocking for debug mode.
    try:
        with open("debug-c95d1a.log", "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        pass
    try:
        import urllib.request
        req = urllib.request.Request(
            "http://127.0.0.1:7727/ingest/1e0cafff-2274-484c-b0a9-f1903ea150e9",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Debug-Session-Id": "c95d1a"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=0.2).read()
    except Exception:
        pass
    # endregion

st.markdown("""
<style>
    .main .block-container {padding-top: 1.4rem; padding-bottom: 1.8rem;}
    .app-hero {
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 0.9rem 1rem;
        background: #f8fafc;
        margin-bottom: 1rem;
    }
    .section-card {
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 0.75rem 0.9rem;
        background: #ffffff;
        margin-bottom: 0.9rem;
    }
    .small-muted {color: #6b7280; font-size: 0.9rem;}
</style>
""", unsafe_allow_html=True)

def _load_auth_users() -> dict[str, str]:
    users: dict[str, str] = {}
    try:
        secret_users = st.secrets.get("auth_users", {})
        if isinstance(secret_users, Mapping):
            for k, v in secret_users.items():
                u = str(k).strip()
                p = str(v).strip()
                if u and p:
                    users[u] = p
    except Exception:
        pass
    env_json = os.getenv("LGD_AUTH_USERS_JSON", "").strip()
    if env_json:
        try:
            parsed = json.loads(env_json)
            if isinstance(parsed, Mapping):
                for k, v in parsed.items():
                    u = str(k).strip()
                    p = str(v).strip()
                    if u and p:
                        users[u] = p
        except Exception:
            pass
    return users

def _load_auth_token_secret() -> str:
    try:
        secret = str(st.secrets.get("auth_token_secret", "")).strip()
        if secret:
            return secret
    except Exception:
        pass
    return os.getenv("LGD_AUTH_TOKEN_SECRET", "").strip()

def _token_encode(payload: dict, secret: str) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).decode("ascii")
    sig = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{body}.{sig}"

def _token_decode(token: str, secret: str) -> dict | None:
    if not token or "." not in token:
        return None
    body, sig = token.rsplit(".", 1)
    expected = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(body.encode("ascii")).decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    exp = payload.get("exp")
    user = payload.get("user")
    if not isinstance(exp, (int, float)) or not isinstance(user, str):
        return None
    if time.time() > float(exp):
        return None
    return payload

def _try_restore_auth_from_token(users: dict[str, str]) -> bool:
    if st.session_state.get("auth_ok"):
        return True
    token = st.query_params.get("auth_token")
    if not token:
        return False
    secret = _load_auth_token_secret()
    if not secret:
        return False
    payload = _token_decode(str(token), secret)
    if not payload:
        return False
    user = str(payload["user"]).strip()
    if user not in users:
        return False
    st.session_state["auth_ok"] = True
    st.session_state["auth_user"] = user
    return True

def _render_auth_gate() -> None:
    users = _load_auth_users()
    if _try_restore_auth_from_token(users):
        return
    if st.session_state.get("auth_ok"):
        return
    st.title("🔐 Authorized Access")
    st.caption("Sign in to use the LGD Fuzzy Matcher.")
    if not users:
        st.error("No authorized users configured. Set `auth_users` in Streamlit secrets or `LGD_AUTH_USERS_JSON` in environment.")
        st.stop()
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        remember_me = st.checkbox("Remember me for 24 hours")
        submit = st.form_submit_button("Sign in", type="primary")
    if submit:
        expected = users.get(str(username).strip())
        if expected and hmac.compare_digest(str(password), expected):
            st.session_state["auth_ok"] = True
            st.session_state["auth_user"] = str(username).strip()
            if remember_me:
                secret = _load_auth_token_secret()
                if secret:
                    token = _token_encode({
                        "user": str(username).strip(),
                        "exp": int(time.time() + 24 * 3600),
                    }, secret)
                    st.query_params["auth_token"] = token
                else:
                    st.info("Set `auth_token_secret` in Streamlit secrets to enable persistent login.")
            st.success("Login successful.")
            st.rerun()
        st.error("Invalid username or password.")
    st.stop()

_render_auth_gate()
is_admin_user = st.session_state.get("auth_user") == "admin"

@st.cache_resource(show_spinner="Building indices...")
def get_matcher_from_bytes(state_bytes: bytes, district_bytes: bytes) -> LGDMatcher:
    m = LGDMatcher(config_path="config.json")
    sdf = pd.read_csv(io.BytesIO(state_bytes), dtype=str)
    ddf = pd.read_csv(io.BytesIO(district_bytes), dtype=str)
    m.load_master_from_dataframes(sdf, ddf)
    return m


def row_style(row):
    colors = {"EXACT":"#dcfce7","HIGH_CONFIDENCE":"#dbeafe",
              "MEDIUM_CONFIDENCE":"#fef3c7","LOW_CONFIDENCE":"#fee2e2","NOT_FOUND":"#f3f4f6"}
    c = colors.get(row.get("match_status",""),"")
    return [f"background-color:{c}"]*len(row)

def suggestion_row_style(row):
    t = str(row.get("type", ""))
    if "PREFIX" in t or "ALL" in t:
        c = "#dcfce7"
    elif "IN_STATE" in t:
        c = "#dbeafe"
    elif "ANY_STATE" in t:
        c = "#fef3c7"
    elif t == "STATE":
        c = "#e0e7ff"
    else:
        c = "#f9fafb"
    return [f"background-color:{c}"] * len(row)


def to_csv(df): return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

def to_sql(df, table):
    tmp = "tmp_sql_export.sql"
    generate_sql_update(df, table_name=table, output_path=tmp)
    with open(tmp,"rb") as f: return f.read()


with st.sidebar:
    st.title("🗺️ LGD Fuzzy Matcher")
    st.caption(f"Signed in as: **{st.session_state.get('auth_user', 'unknown')}**")
    if st.button("Sign out", use_container_width=True):
        st.session_state["auth_ok"] = False
        st.session_state["auth_user"] = ""
        st.query_params.clear()
        st.rerun()
    st.caption("Indian Local Government Directory")
    st.divider()
    st.subheader("Master Data Source")
    if not is_admin_user:
        st.info("Only `admin` can change master data source settings.")
    state_up = st.file_uploader("State master CSV",    type="csv", key="su", disabled=not is_admin_user)
    dist_up  = st.file_uploader("District master CSV", type="csv", key="du", disabled=not is_admin_user)
    use_local = st.checkbox("Use local CSVs", value=True, disabled=not is_admin_user)
    if not is_admin_user:
        # Enforce read-only data source behavior for non-admin users.
        use_local = True
    st.divider()
    st.subheader("Thresholds")
    high_t   = st.slider("HIGH >= ",   80, 99, 90)
    medium_t = st.slider("MEDIUM >= ", 60, 89, 75)
    low_t    = st.slider("LOW >= ",    40, 74, 60)
    sql_table = st.text_input("SQL Table Name", "target_table")
    st.divider()
    st.subheader("Quick Options")
    top_n = st.slider("Suggestions: top N", 1, 10, 5)
    show_suggestions = st.checkbox("Show suggestions", value=True)


st.title("🗺️ LGD Fuzzy Matching System")
st.markdown("""
<div class="app-hero">
  <strong>Map raw state and district names to official LGD codes</strong><br/>
  <span class="small-muted">Use Quick Validate for spot checks, then run bulk mapping from Upload & Match.</span>
</div>
""", unsafe_allow_html=True)

tab0, tab1, tab2, tab3 = st.tabs(["🔎 Quick Validate", "📤 Upload & Match", "📊 Results & Download", "📖 Help"])

def load_matcher_from_sources() -> LGDMatcher | None:
    try:
        if state_up and dist_up:
            sb, db = state_up.getvalue(), dist_up.getvalue()
        elif use_local and os.path.exists("lgd_STATE.csv") and os.path.exists("DISTRICT_STATE.csv"):
            sb, db = open("lgd_STATE.csv", "rb").read(), open("DISTRICT_STATE.csv", "rb").read()
        else:
            return None
        m = get_matcher_from_bytes(sb, db)
        m.thresholds.update({
            "high_confidence": high_t,
            "medium_confidence": medium_t,
            "low_confidence": low_t,
        })
        return m
    except Exception:
        return None

def split_csv_values(s: str) -> list[str]:
    if s is None:
        return []
    parts = [p.strip() for p in str(s).split(",")]
    return [p for p in parts if p]

def build_rows(state_name: str, state_lgd: str, dist_name: str, dist_lgd: str) -> list[dict]:
    a = split_csv_values(state_name)
    b = split_csv_values(state_lgd)
    c = split_csv_values(dist_name)
    d = split_csv_values(dist_lgd)
    n = max(len(a), len(b), len(c), len(d), 1)

    def expand(lst: list[str]) -> list[str]:
        if not lst:
            return [""] * n
        if len(lst) == 1 and n > 1:
            return lst * n
        if len(lst) < n:
            return lst + [""] * (n - len(lst))
        return lst[:n]

    a, b, c, d = expand(a), expand(b), expand(c), expand(d)
    rows = []
    for i in range(n):
        rows.append({
            "id": str(i + 1),
            "state_name_in": a[i],
            "state_lgd_in": b[i],
            "district_name_in": c[i],
            "district_lgd_in": d[i],
        })
    return rows

def state_from_lgd(matcher: LGDMatcher, state_lgd_code: str) -> dict | None:
    if not state_lgd_code:
        return None
    df = matcher.state_df
    if df is None:
        return None
    code = str(state_lgd_code).strip()
    hit = df[df["state_lgd_code"].astype(str).str.strip() == code]
    if hit.empty:
        return None
    r = hit.iloc[0]
    return {"state_lgd_code": code, "state_name": str(r["state_name"]).strip()}

def district_from_lgd(matcher: LGDMatcher, district_lgd_code: str, state_lgd_code: str | None = None) -> dict | None:
    if not district_lgd_code:
        return None
    df = matcher.district_df
    if df is None:
        return None
    dc = str(district_lgd_code).strip()
    ddf = df.copy()
    ddf["district_lgd_code"] = ddf["district_lgd_code"].astype(str).str.strip()
    if state_lgd_code:
        sc = str(state_lgd_code).strip()
        ddf["state_lgd_code"] = ddf["state_lgd_code"].astype(str).str.strip()
        hit = ddf[(ddf["district_lgd_code"] == dc) & (ddf["state_lgd_code"] == sc)]
    else:
        hit = ddf[ddf["district_lgd_code"] == dc]
    if hit.empty:
        return None
    r = hit.iloc[0]
    return {
        "district_lgd_code": dc,
        "district_name": str(r["district_name"]).strip(),
        "state_lgd_code": str(r["state_lgd_code"]).strip(),
    }

def list_districts_safe(matcher: LGDMatcher, state_lgd_code: str) -> list[dict]:
    sc = str(state_lgd_code).strip() if state_lgd_code is not None else ""
    if not sc:
        return []
    list_fn = getattr(matcher, "list_districts", None)
    if callable(list_fn):
        st.session_state["district_list_fallback_used"] = False
        return list_fn(sc)
    st.session_state["district_list_fallback_used"] = True
    df = getattr(matcher, "district_df", None)
    if df is None:
        return []
    ddf = df.copy()
    ddf["state_lgd_code"] = ddf["state_lgd_code"].astype(str).str.strip()
    ddf["district_lgd_code"] = ddf["district_lgd_code"].astype(str).str.strip()
    ddf["district_name"] = ddf["district_name"].astype(str).str.strip()
    ddf = ddf[(ddf["state_lgd_code"] == sc) & (ddf["district_lgd_code"] != "") & (ddf["district_name"] != "")]
    ddf = ddf[["district_lgd_code", "district_name"]].drop_duplicates().sort_values(["district_name", "district_lgd_code"])
    return ddf.to_dict(orient="records")

def suggest_states_safe(matcher: LGDMatcher, raw_state: str, limit: int = 5) -> list[dict]:
    suggest_fn = getattr(matcher, "suggest_states", None)
    if callable(suggest_fn):
        st.session_state["state_suggest_fallback_used"] = False
        return suggest_fn(raw_state, limit=limit)
    st.session_state["state_suggest_fallback_used"] = True
    match_fn = getattr(matcher, "match_state", None)
    if not callable(match_fn):
        return []
    sm = match_fn(raw_state)
    sc = sm.get("state_lgd_code")
    if sc is None:
        return []
    return [{
        "state_lgd_code": sc,
        "state_name": sm.get("state_name_corrected"),
        "score": sm.get("state_score", 0.0),
        "status": sm.get("state_status", "NOT_FOUND"),
    }]

def suggest_districts_safe(matcher: LGDMatcher, raw_district: str, state_lgd_code: str | None, limit: int = 5) -> list[dict]:
    suggest_fn = getattr(matcher, "suggest_districts", None)
    if callable(suggest_fn):
        st.session_state["district_suggest_fallback_used"] = False
        return suggest_fn(raw_district, state_lgd_code=state_lgd_code, limit=limit)
    st.session_state["district_suggest_fallback_used"] = True
    if not state_lgd_code:
        return []
    match_fn = getattr(matcher, "match_district", None)
    if not callable(match_fn):
        return []
    dm = match_fn(raw_district, str(state_lgd_code))
    dc = dm.get("district_lgd_code")
    if dc is None:
        return []
    return [{
        "district_lgd_code": dc,
        "district_name": dm.get("district_name_corrected"),
        "state_lgd_code": str(state_lgd_code),
        "score": dm.get("district_score", 0.0),
        "status": dm.get("district_status", "NOT_FOUND"),
    }]

def district_prefix_list_in_state(matcher: LGDMatcher, state_lgd_code: str, prefix: str) -> list[dict]:
    districts = list_districts_safe(matcher, state_lgd_code)
    p = (prefix or "").strip().lower()
    if not p:
        return districts
    return [d for d in districts if str(d.get("district_name", "")).strip().lower().startswith(p)]

with tab0:
    st.subheader("Quick Validate")
    st.caption("Fill any 1–4 fields. You can enter multiple values separated by commas.")
    st.info("Supported combinations: state/district name, state/district LGD code, or any mix. If exact mapping is not possible, suggestions are shown.")
    st.markdown(
        "<span style='background:#dcfce7;padding:2px 8px;border-radius:6px;'>EXACT</span> "
        "<span style='background:#dbeafe;padding:2px 8px;border-radius:6px;'>HIGH</span> "
        "<span style='background:#fef3c7;padding:2px 8px;border-radius:6px;'>MEDIUM</span> "
        "<span style='background:#fee2e2;padding:2px 8px;border-radius:6px;'>LOW</span> "
        "<span style='background:#f3f4f6;padding:2px 8px;border-radius:6px;'>NOT_FOUND</span>",
        unsafe_allow_html=True,
    )

    matcher = load_matcher_from_sources()
    if matcher is None:
        st.info("Upload master CSVs in the sidebar or enable 'Use local CSVs' to use Quick Validate.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            q_state_name = st.text_input("State name (optional)", placeholder="e.g. UP, Delhi, uttaranchal")
            q_dist_name = st.text_input("District name (optional)", placeholder="e.g. varansi, new delhi")
        with c2:
            q_state_lgd = st.text_input("State LGD code (optional)", placeholder="e.g. 9, 7")
            q_dist_lgd = st.text_input("District LGD code (optional)", placeholder="e.g. 187, 141")

        run_quick = st.button("Validate", type="primary")
        if run_quick:
            st.session_state["debug_run_id"] = str(uuid.uuid4())
            rows = build_rows(q_state_name, q_state_lgd, q_dist_name, q_dist_lgd)
            _debug_log(
                "H1",
                "app.py:quick_validate_start",
                "Quick validate input snapshot",
                {
                    "rows_count": len(rows),
                    "state_name": q_state_name,
                    "district_name": q_dist_name,
                    "state_lgd": q_state_lgd,
                    "district_lgd": q_dist_lgd,
                },
            )
            outputs = []
            sugg_rows = []

            for r in rows:
                has_state_name = bool((r["state_name_in"] or "").strip())
                has_state_lgd = bool((r["state_lgd_in"] or "").strip())
                has_dist_name = bool((r["district_name_in"] or "").strip())
                has_dist_lgd = bool((r["district_lgd_in"] or "").strip())

                if not any([has_state_name, has_state_lgd, has_dist_name, has_dist_lgd]):
                    outputs.append({
                        "id": r["id"],
                        "state_name_raw": "",
                        "district_name_raw": "",
                        "state_lgd_code": None,
                        "state_name_corrected": None,
                        "district_lgd_code": None,
                        "district_name_corrected": None,
                        "match_confidence_score": 0.0,
                        "match_status": "NOT_FOUND",
                        "resolution_note": "No input provided in this row.",
                    })
                    continue

                # Resolve from LGD codes if provided (these are exact validations)
                state_by_code = state_from_lgd(matcher, r["state_lgd_in"])
                district_by_code = district_from_lgd(matcher, r["district_lgd_in"], state_lgd_code=r["state_lgd_in"] or None)
                inferred_state_from_district = None
                if district_by_code and not state_by_code:
                    inferred_state_from_district = state_from_lgd(matcher, district_by_code.get("state_lgd_code"))
                    if inferred_state_from_district:
                        state_by_code = inferred_state_from_district
                _debug_log(
                    "H2",
                    "app.py:quick_validate_code_resolution",
                    "Code lookup results",
                    {
                        "row_id": r["id"],
                        "state_lgd_in": r["state_lgd_in"],
                        "district_lgd_in": r["district_lgd_in"],
                        "state_by_code_found": bool(state_by_code),
                        "district_by_code_found": bool(district_by_code),
                        "inferred_state_from_district": bool(inferred_state_from_district),
                        "district_by_code_state_lgd": (district_by_code or {}).get("state_lgd_code"),
                        "district_by_code_name": (district_by_code or {}).get("district_name"),
                    },
                )

                # Prefer user-provided state name; else use state name from state LGD code; else blank
                state_name_raw = (r["state_name_in"] or (state_by_code["state_name"] if state_by_code else "")).strip()
                district_name_raw = (r["district_name_in"] or (district_by_code["district_name"] if district_by_code else "")).strip()

                df = pd.DataFrame([{
                    "id": r["id"],
                    "state_name_raw": state_name_raw,
                    "district_name_raw": district_name_raw,
                }], dtype=str)
                res = matcher.match_dataframe(df).iloc[0].to_dict()

                # If user provided LGD codes, verify them and surface as guidance fields
                res["state_lgd_input"] = r["state_lgd_in"] or None
                res["district_lgd_input"] = r["district_lgd_in"] or None
                res["state_lgd_input_valid"] = bool(state_by_code) if r["state_lgd_in"] else None
                res["district_lgd_input_valid"] = bool(district_by_code) if r["district_lgd_in"] else None
                if district_by_code and state_by_code and str(district_by_code["state_lgd_code"]) != str(state_by_code["state_lgd_code"]):
                    res["district_state_mismatch"] = True
                else:
                    res["district_state_mismatch"] = False if (district_by_code and state_by_code) else None

                note_parts = []
                if has_state_lgd and not state_by_code:
                    note_parts.append("State LGD code not found.")
                if has_dist_lgd and not district_by_code:
                    note_parts.append("District LGD code not found (or not in the given state).")
                if res.get("district_state_mismatch"):
                    note_parts.append("State LGD and District LGD belong to different states.")
                if district_by_code and inferred_state_from_district:
                    note_parts.append("State inferred from district LGD code.")
                if not note_parts and (has_state_lgd or has_dist_lgd):
                    note_parts.append("Resolved using provided LGD code(s).")
                if not note_parts and (has_state_name or has_dist_name):
                    note_parts.append("Resolved using name matching.")
                res["resolution_note"] = " ".join(note_parts)

                # If LGD codes are valid, treat them as authoritative and lock exact output.
                if district_by_code and state_by_code and not res.get("district_state_mismatch"):
                    res["state_lgd_code"] = state_by_code["state_lgd_code"]
                    res["state_name_corrected"] = state_by_code["state_name"]
                    res["district_lgd_code"] = district_by_code["district_lgd_code"]
                    res["district_name_corrected"] = district_by_code["district_name"]
                    res["match_confidence_score"] = 100.0
                    res["match_status"] = "EXACT"
                _debug_log(
                    "H3",
                    "app.py:quick_validate_final_row",
                    "Quick validate output row",
                    {
                        "row_id": r["id"],
                        "match_status": res.get("match_status"),
                        "state_lgd_code": res.get("state_lgd_code"),
                        "district_lgd_code": res.get("district_lgd_code"),
                        "state_name_corrected": res.get("state_name_corrected"),
                        "district_name_corrected": res.get("district_name_corrected"),
                        "district_state_mismatch": res.get("district_state_mismatch"),
                    },
                )

                outputs.append(res)

                sc_best = res.get("state_lgd_code")
                has_state_scoped_need = bool(sc_best and ((r["district_name_in"] or "").strip() or not (r["district_name_in"] or "").strip()))
                _debug_log(
                    "H6",
                    "app.py:suggestions_branch_gate",
                    "Suggestion branch gate values",
                    {
                        "row_id": r["id"],
                        "show_suggestions": bool(show_suggestions),
                        "state_lgd_code_best": sc_best,
                        "district_name_in": r["district_name_in"],
                        "state_scoped_need": has_state_scoped_need,
                    },
                )

                # Always produce state-scoped district options when a state is resolved.
                # This keeps quick-validate useful even if "Show suggestions" is off.
                if sc_best:
                    if r["district_name_in"]:
                        pref_rows = district_prefix_list_in_state(matcher, str(sc_best), r["district_name_in"])
                        _debug_log(
                            "H7",
                            "app.py:district_prefix_rows",
                            "State-scoped prefix rows generated",
                            {"row_id": r["id"], "state_lgd_code": str(sc_best), "prefix": r["district_name_in"], "count": len(pref_rows)},
                        )
                        if pref_rows:
                            for d in pref_rows:
                                sugg_rows.append({
                                    "id": r["id"],
                                    "type": "DISTRICT_IN_STATE_PREFIX",
                                    "district_lgd_code": d.get("district_lgd_code"),
                                    "district_name": d.get("district_name"),
                                    "state_lgd_code": str(sc_best),
                                })
                        elif show_suggestions:
                            for d in suggest_districts_safe(matcher, r["district_name_in"], state_lgd_code=sc_best, limit=top_n):
                                sugg_rows.append({"id": r["id"], "type": "DISTRICT_IN_STATE", **d})
                    else:
                        all_rows = district_prefix_list_in_state(matcher, str(sc_best), "")
                        _debug_log(
                            "H7",
                            "app.py:district_all_rows",
                            "State-scoped all-district rows generated",
                            {"row_id": r["id"], "state_lgd_code": str(sc_best), "count": len(all_rows)},
                        )
                        for d in all_rows:
                            sugg_rows.append({
                                "id": r["id"],
                                "type": "DISTRICT_IN_STATE_ALL",
                                "district_lgd_code": d.get("district_lgd_code"),
                                "district_name": d.get("district_name"),
                                "state_lgd_code": str(sc_best),
                            })

                if show_suggestions:
                    if r["state_name_in"]:
                        for s in suggest_states_safe(matcher, r["state_name_in"], limit=top_n):
                            sugg_rows.append({"id": r["id"], "type": "STATE", **s})
                    if not sc_best and r["district_name_in"]:
                        for d in suggest_districts_safe(matcher, r["district_name_in"], state_lgd_code=None, limit=top_n):
                            sugg_rows.append({"id": r["id"], "type": "DISTRICT_ANY_STATE", **d})

            out_df = pd.DataFrame(outputs)
            st.markdown("### Results")
            if not out_df.empty and "match_status" in out_df.columns:
                c1, c2, c3, c4, c5 = st.columns(5)
                counts = out_df["match_status"].value_counts()
                c1.metric("EXACT", int(counts.get("EXACT", 0)))
                c2.metric("HIGH", int(counts.get("HIGH_CONFIDENCE", 0)))
                c3.metric("MEDIUM", int(counts.get("MEDIUM_CONFIDENCE", 0)))
                c4.metric("LOW", int(counts.get("LOW_CONFIDENCE", 0)))
                c5.metric("NOT FOUND", int(counts.get("NOT_FOUND", 0)))
            st.dataframe(out_df.style.apply(row_style, axis=1), use_container_width=True, height=320)

            _debug_log(
                "H8",
                "app.py:suggestions_render_gate",
                "Suggestions render gate values",
                {"show_suggestions": bool(show_suggestions), "sugg_rows_count": len(sugg_rows)},
            )
            if sugg_rows:
                st.divider()
                st.markdown("### Suggestions (for user decision)")
                if st.session_state.get("state_suggest_fallback_used") or st.session_state.get("district_suggest_fallback_used"):
                    st.info("Using compatibility suggestion mode (matcher suggestion methods unavailable in this runtime).")
                sugg_df = pd.DataFrame(sugg_rows)
                st.dataframe(sugg_df.style.apply(suggestion_row_style, axis=1), use_container_width=True, height=360)

        st.divider()
        st.subheader("List all districts of a state")
        st.caption("Enter state name/alias or LGD code. Example: `up` or `9`.")
        list_state = st.text_input("State for district list", key="list_state_quick")
        if list_state.strip():
            sm = matcher.match_state(list_state.strip())
            sc = sm.get("state_lgd_code")
            if sc is None:
                st.warning("State not found.")
                if show_suggestions:
                    srows = suggest_states_safe(matcher, list_state, limit=top_n)
                    if st.session_state.get("state_suggest_fallback_used"):
                        st.info("Using compatibility suggestion mode (matcher suggestion methods unavailable in this runtime).")
                    st.dataframe(pd.DataFrame(srows), use_container_width=True, height=220)
            else:
                dlist = list_districts_safe(matcher, str(sc))
                if st.session_state.get("district_list_fallback_used"):
                    st.info("Using compatibility district listing mode (matcher method unavailable in this runtime).")
                st.caption(f"State: **{sm.get('state_name_corrected')}** (LGD {sc}) | Districts: {len(dlist)}")
                st.dataframe(pd.DataFrame(dlist), use_container_width=True, height=320)

with tab1:
    st.markdown('<div class="section-card"><strong>Bulk Upload Workflow</strong><br/><span class="small-muted">1) Upload file  2) Map columns  3) Run LGD matching  4) Download output</span></div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Upload Input File")
        inp = st.file_uploader("CSV or Excel", type=["csv","xlsx","xls"])
        if inp:
            raw_df = pd.read_excel(inp, dtype=str) if inp.name.endswith((".xlsx",".xls")) else pd.read_csv(inp, dtype=str)
            st.success(f"Loaded {len(raw_df):,} rows")
            st.dataframe(raw_df.head(5), use_container_width=True)
    with col2:
        st.subheader("Sample Format")
        sample = pd.DataFrame({
            "id":[1,2,3,4,5],
            "state_name_raw":["delhii","NCT Delhi","UP","Bengluru","west bengall"],
            "district_name_raw":["New Delhi","District Agra","varansi","bangalore","calcuta"]
        })
        st.dataframe(sample, use_container_width=True)
        st.download_button("Download Sample CSV", to_csv(sample), "sample_input.csv","text/csv")

    # ── Column Mapping (shown only when file is loaded) ──────────────────────
    if inp and "raw_df" in dir():
        st.divider()
        st.subheader("📌 Map Your Columns")
        st.caption("Select which columns in your file contain State and District names.")

        all_cols = list(raw_df.columns)
        none_opt = ["-- Not in file --"]
        col_options = none_opt + all_cols

        c1, c2, c3 = st.columns(3)
        with c1:
            # Try to auto-detect state column
            auto_state = next(
                (c for c in all_cols if any(k in c.lower() for k in ["state_name_raw","state_name","state"])),
                all_cols[0] if all_cols else None
            )
            state_col = st.selectbox(
                "State Name Column",
                all_cols,
                index=all_cols.index(auto_state) if auto_state in all_cols else 0,
                key="state_col"
            )
        with c2:
            # Try to auto-detect district column
            auto_dist = next(
                (c for c in all_cols if any(k in c.lower() for k in ["district_name_raw","district_name","district"])),
                all_cols[1] if len(all_cols) > 1 else all_cols[0]
            )
            dist_col = st.selectbox(
                "District Name Column",
                all_cols,
                index=all_cols.index(auto_dist) if auto_dist in all_cols else 0,
                key="dist_col"
            )
        with c3:
            auto_id = next(
                (c for c in all_cols if c.lower() in ["id","sr","sno","s_no","serial"]),
                None
            )
            id_col = st.selectbox(
                "ID Column (optional)",
                col_options,
                index=col_options.index(auto_id) if auto_id in col_options else 0,
                key="id_col"
            )

        # Build mapped dataframe preview
        mapped_df = raw_df.copy()
        mapped_df["state_name_raw"]    = raw_df[state_col].fillna("").astype(str)
        mapped_df["district_name_raw"] = raw_df[dist_col].fillna("").astype(str)
        if id_col != "-- Not in file --":
            mapped_df["id"] = raw_df[id_col].astype(str)

        with st.expander("Preview mapped columns"):
            preview_cols = []
            if "id" in mapped_df.columns:
                preview_cols.append("id")
            preview_cols += [c for c in ["state_name_raw", "district_name_raw"] if c in mapped_df.columns]
            st.dataframe(
                mapped_df[preview_cols].head(10),
                use_container_width=True
            )

        st.divider()
        run = st.button("Run LGD Matching", type="primary")

        if run:
            with st.spinner("Loading master data..."):
                try:
                    if state_up and dist_up:
                        sb, db = state_up.read(), dist_up.read()
                    elif use_local and os.path.exists("lgd_STATE.csv") and os.path.exists("DISTRICT_STATE.csv"):
                        sb = open("lgd_STATE.csv","rb").read()
                        db = open("DISTRICT_STATE.csv","rb").read()
                    else:
                        st.error("Master CSVs not found. Upload in sidebar or enable 'Use local CSVs'."); st.stop()
                    matcher = get_matcher_from_bytes(sb, db)
                    matcher.thresholds.update({
                        "high_confidence": high_t,
                        "medium_confidence": medium_t,
                        "low_confidence": low_t
                    })
                except Exception as e:
                    st.error(f"Master load failed: {e}"); st.stop()

            with st.spinner(f"Matching {len(mapped_df):,} rows..."):
                t0 = time.perf_counter()
                results = matcher.match_dataframe(mapped_df)
                elapsed = time.perf_counter() - t0

            st.session_state["results"] = results
            st.session_state["matcher"] = matcher
            counts = results["match_status"].value_counts()
            st.success(f"✅ Done in {elapsed:.2f}s ({len(results)/max(elapsed,0.001):.0f} rows/sec)")
            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("EXACT",     counts.get("EXACT",0))
            c2.metric("HIGH",      counts.get("HIGH_CONFIDENCE",0))
            c3.metric("MEDIUM",    counts.get("MEDIUM_CONFIDENCE",0))
            c4.metric("LOW",       counts.get("LOW_CONFIDENCE",0))
            c5.metric("NOT FOUND", counts.get("NOT_FOUND",0))
            st.info("Go to **Results & Download** tab for full results.")
    else:
        st.button("Run LGD Matching", type="primary", disabled=True)


with tab2:
    if "results" not in st.session_state:
        st.info("Run the matcher first in Upload & Match tab.")
    else:
        results = st.session_state["results"]
        st.subheader("Matched Results")
        filter_s = st.multiselect("Filter by Status",
            ["EXACT","HIGH_CONFIDENCE","MEDIUM_CONFIDENCE","LOW_CONFIDENCE","NOT_FOUND"],
            default=["EXACT","HIGH_CONFIDENCE","MEDIUM_CONFIDENCE","LOW_CONFIDENCE","NOT_FOUND"])
        view = results[results["match_status"].isin(filter_s)]
        st.dataframe(view.style.apply(row_style,axis=1), use_container_width=True, height=420)
        st.caption(f"Showing {len(view):,} of {len(results):,} rows")

        st.divider()
        st.subheader("Manual Correction")
        uncertain = results[results["match_status"].isin(["LOW_CONFIDENCE","NOT_FOUND"])]
        if uncertain.empty:
            st.success("No uncertain rows — all matched with medium confidence or above!")
        else:
            matcher = st.session_state.get("matcher")
            state_names = sorted(results["state_name_corrected"].dropna().unique().tolist())
            sel_id = st.selectbox("Select Row ID", uncertain["id"].tolist())
            row_idx = results.index[results["id"]==sel_id][0]
            c1,c2 = st.columns(2)
            with c1:
                sel_state = st.selectbox("Correct State", ["(keep)"] + state_names)
            with c2:
                dist_opts = []
                if sel_state != "(keep)" and matcher:
                    sc = results.loc[results["state_name_corrected"]==sel_state,"state_lgd_code"]
                    if len(sc):
                        sc_code = sc.iloc[0]
                        dist_opts = sorted([v["district_name"] for v in matcher.district_norm_by_state.get(sc_code,{}).values()])
                sel_dist = st.selectbox("Correct District", ["(keep)"] + dist_opts)

            if st.button("Apply Correction"):
                if sel_state != "(keep)":
                    sc_row = results.loc[results["state_name_corrected"]==sel_state,"state_lgd_code"]
                    if len(sc_row):
                        results.at[row_idx,"state_lgd_code"] = sc_row.iloc[0]
                        results.at[row_idx,"state_name_corrected"] = sel_state
                if sel_dist != "(keep)" and matcher:
                    sc2 = results.at[row_idx,"state_lgd_code"]
                    for rec in matcher.district_norm_by_state.get(sc2,{}).values():
                        if rec["district_name"] == sel_dist:
                            results.at[row_idx,"district_lgd_code"] = rec["district_lgd_code"]
                            results.at[row_idx,"district_name_corrected"] = sel_dist
                            results.at[row_idx,"match_status"] = "HIGH_CONFIDENCE"
                            results.at[row_idx,"match_confidence_score"] = 100.0
                            break
                st.session_state["results"] = results
                st.success("Correction applied!"); st.rerun()

        st.divider()
        st.subheader("Downloads")
        d1,d2,d3 = st.columns(3)
        with d1:
            st.download_button("📥 All Results (CSV)", to_csv(results),
                               "lgd_matched_output.csv","text/csv", use_container_width=True)
        with d2:
            unm = results[results["match_status"]=="NOT_FOUND"]
            st.download_button(f"❌ Unmatched ({len(unm)})", to_csv(unm),
                               "lgd_unmatched.csv","text/csv", use_container_width=True)
        with d3:
            st.download_button("📝 SQL UPDATE Script", to_sql(results,sql_table),
                               "lgd_updates.sql","text/plain", use_container_width=True)


with tab3:
    st.markdown("""
### Required Input Columns
Your CSV/Excel **can have any column names** — you just map them in the UI!

| What is needed | Your column could be named... |
|----------------|-------------------------------|
| `state_name_raw` | `state_name`, `State`, `state`, etc. |
| `district_name_raw` | `district_name`, `District`, `dist`, etc. |
| `id` | `id`, `sr`, `sno`, `serial` (optional) |

### Match Status Guide
| Status | Score | Action |
|--------|-------|--------|
| EXACT | 100 | No action needed ✅ |
| HIGH_CONFIDENCE | >= 90 | Trust the result ✅ |
| MEDIUM_CONFIDENCE | 75-89 | Spot-check 🔍 |
| LOW_CONFIDENCE | 60-74 | Manual review ⚠️ |
| NOT_FOUND | < 60 | Must correct ❌ |

### CLI Usage
```bash
python main.py --input your_data.csv --output results.csv
```

### FastAPI Usage
```bash
uvicorn api:app --reload
# POST http://localhost:8000/match
```
""")
