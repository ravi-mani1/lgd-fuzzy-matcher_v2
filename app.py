"""
app.py - Streamlit UI for LGD Fuzzy Matcher
Run: streamlit run app.py
"""
import io, os, time
import pandas as pd
import streamlit as st
from matcher import LGDMatcher
from utils import generate_sql_update, load_config

st.set_page_config(page_title="LGD Fuzzy Matcher", page_icon="🗺️", layout="wide")

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


def to_csv(df): return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

def to_sql(df, table):
    tmp = "tmp_sql_export.sql"
    generate_sql_update(df, table_name=table, output_path=tmp)
    with open(tmp,"rb") as f: return f.read()


with st.sidebar:
    st.title("🗺️ LGD Fuzzy Matcher")
    st.caption("Indian Local Government Directory")
    st.divider()
    st.subheader("Master Data Source")
    state_up = st.file_uploader("State master CSV",    type="csv", key="su")
    dist_up  = st.file_uploader("District master CSV", type="csv", key="du")
    use_local = st.checkbox("Use local CSVs", value=True)
    st.divider()
    st.subheader("Thresholds")
    high_t   = st.slider("HIGH >= ",   80, 99, 90)
    medium_t = st.slider("MEDIUM >= ", 60, 89, 75)
    low_t    = st.slider("LOW >= ",    40, 74, 60)
    sql_table = st.text_input("SQL Table Name", "target_table")


st.title("🗺️ LGD Fuzzy Matching System")
st.markdown("Map raw state/district names → **official LGD codes** with confidence scoring.")

tab1, tab2, tab3 = st.tabs(["📤 Upload & Match", "📊 Results & Download", "📖 Help"])

with tab1:
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
            st.dataframe(
                mapped_df[["id"] + [c for c in ["state_name_raw","district_name_raw"] if c in mapped_df.columns]].head(10),
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
