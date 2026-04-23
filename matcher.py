"""matcher.py - Core LGD Fuzzy Matching Engine."""
from __future__ import annotations
import logging
import json
import time
import urllib.request
from functools import lru_cache
from typing import Any, Optional
import pandas as pd
from rapidfuzz import fuzz, process
from utils import load_config, normalize_alias_map, normalize_text, is_blank

logger = logging.getLogger("lgd_matcher")

def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # region agent log
    payload = {
        "sessionId": "c95d1a",
        "runId": "pre-fix-runtime",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
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


class LGDMatcher:
    """
    Production-grade LGD fuzzy matcher.
    Pipeline: Exact -> Normalize -> Alias -> Fuzzy (rapidfuzz) -> State-first -> Score
    Optimized for 100K+ rows via pre-indexed dicts + lru_cache on unique values.
    """

    def __init__(self, config_path: str = "config.json") -> None:
        self.config = load_config(config_path)
        self.thresholds = self.config["thresholds"]
        self.stop_words = self.config.get("stop_words", [])
        self.state_aliases = normalize_alias_map(self.config.get("state_aliases", {}), self.stop_words)
        self.district_aliases = normalize_alias_map(self.config.get("district_aliases", {}), self.stop_words)
        self.state_df: Optional[pd.DataFrame] = None
        self.district_df: Optional[pd.DataFrame] = None
        self.state_exact_map: dict = {}
        self.state_norm_map: dict = {}
        self.state_choices: list = []
        self.district_exact_by_state: dict = {}
        self.district_norm_by_state: dict = {}
        self.district_choices_by_state: dict = {}
        self.global_district_exact_map: dict = {}
        self.global_district_norm_map: dict = {}
        self.global_district_choices: list = []

    def load_master_from_csv(self, state_csv: str, district_csv: str) -> None:
        self.state_df = pd.read_csv(state_csv, dtype=str).fillna("")
        self.district_df = pd.read_csv(district_csv, dtype=str).fillna("")
        self.state_df = self.state_df.rename(columns={"state_lgd": "state_lgd_code"})
        self.district_df = self.district_df.rename(columns={"state_lgd": "state_lgd_code", "district_lgd": "district_lgd_code"})
        self._validate_master_columns()
        self._build_indices()

    def load_master_from_mysql(self, mysql_cfg: dict = None) -> None:
        cfg = mysql_cfg or self.config.get("mysql", {})
        if not cfg.get("enabled", False):
            raise RuntimeError("MySQL disabled in config.json. Set mysql.enabled=true.")
        try:
            import pymysql
        except ImportError:
            raise RuntimeError("Run: pip install pymysql")
        conn = pymysql.connect(host=cfg["host"], port=int(cfg["port"]),
                               user=cfg["user"], password=cfg["password"],
                               database=cfg["database"], charset="utf8mb4")
        self.state_df = pd.read_sql("SELECT state_lgd_code, state_name FROM state_master", conn).fillna("")
        self.district_df = pd.read_sql(
            "SELECT district_lgd_code, district_name, state_lgd_code FROM district_master", conn).fillna("")
        conn.close()
        self._validate_master_columns()
        self._build_indices()

    def load_master_from_dataframes(self, state_df: pd.DataFrame, district_df: pd.DataFrame) -> None:
        self.state_df = state_df.copy().fillna("").rename(columns={"state_lgd": "state_lgd_code"})
        self.district_df = district_df.copy().fillna("").rename(columns={
            "state_lgd": "state_lgd_code", "district_lgd": "district_lgd_code"})
        self._validate_master_columns()
        self._build_indices()

    def _validate_master_columns(self) -> None:
        for df, required, label in [
            (self.state_df, {"state_lgd_code", "state_name"}, "State"),
            (self.district_df, {"district_lgd_code", "district_name", "state_lgd_code"}, "District"),
        ]:
            missing = required - set(df.columns)
            if missing:
                raise ValueError(f"{label} master missing columns: {sorted(missing)}")

    def _build_indices(self) -> None:
        logger.info("Building indices...")
        for _, row in self.state_df.iterrows():
            rec = {"state_lgd_code": str(row["state_lgd_code"]).strip(),
                   "state_name": str(row["state_name"]).strip()}
            self.state_exact_map[rec["state_name"].lower()] = rec
            self.state_norm_map[normalize_text(rec["state_name"], self.stop_words)] = rec
        self.state_choices = list(self.state_norm_map.keys())

        for _, row in self.district_df.iterrows():
            rec = {"district_lgd_code": str(row["district_lgd_code"]).strip(),
                   "district_name": str(row["district_name"]).strip(),
                   "state_lgd_code": str(row["state_lgd_code"]).strip()}
            sc = rec["state_lgd_code"]
            raw_k = rec["district_name"].lower()
            norm_k = normalize_text(rec["district_name"], self.stop_words)
            self.district_exact_by_state.setdefault(sc, {})[raw_k] = rec
            self.district_norm_by_state.setdefault(sc, {})[norm_k] = rec
            self.global_district_exact_map.setdefault(raw_k, []).append(rec)
            self.global_district_norm_map.setdefault(norm_k, []).append(rec)

        self.district_choices_by_state = {sc: list(d.keys()) for sc, d in self.district_norm_by_state.items()}
        self.global_district_choices = list(self.global_district_norm_map.keys())
        logger.info("Indices ready | states=%d | districts=%d", len(self.state_df), len(self.district_df))

    def list_states(self) -> list[dict]:
        if self.state_df is None:
            raise RuntimeError("Master data not loaded. Call load_master_from_csv/load_master_from_mysql first.")
        out = (
            self.state_df[["state_lgd_code", "state_name"]]
            .dropna()
            .astype(str)
            .assign(
                state_lgd_code=lambda d: d["state_lgd_code"].str.strip(),
                state_name=lambda d: d["state_name"].str.strip(),
            )
        )
        out = out[(out["state_lgd_code"] != "") & (out["state_name"] != "")]
        out = out.drop_duplicates().sort_values(["state_name", "state_lgd_code"])
        return out.to_dict(orient="records")

    def list_districts(self, state_lgd_code: str) -> list[dict]:
        if self.district_df is None:
            raise RuntimeError("Master data not loaded. Call load_master_from_csv/load_master_from_mysql first.")
        sc = "" if is_blank(state_lgd_code) else str(state_lgd_code).strip()
        if not sc:
            return []
        df = self.district_df.copy()
        df["state_lgd_code"] = df["state_lgd_code"].astype(str).str.strip()
        df["district_lgd_code"] = df["district_lgd_code"].astype(str).str.strip()
        df["district_name"] = df["district_name"].astype(str).str.strip()
        df = df[(df["state_lgd_code"] == sc) & (df["district_lgd_code"] != "") & (df["district_name"] != "")]
        df = df[["district_lgd_code", "district_name"]].drop_duplicates().sort_values(["district_name", "district_lgd_code"])
        return df.to_dict(orient="records")

    def _status(self, score: float, exact: bool = False) -> str:
        if exact: return "EXACT"
        t = self.thresholds
        if score >= t["high_confidence"]: return "HIGH_CONFIDENCE"
        if score >= t["medium_confidence"]: return "MEDIUM_CONFIDENCE"
        if score >= t["low_confidence"]: return "LOW_CONFIDENCE"
        return "NOT_FOUND"

    def _best_fuzzy(self, query: str, choices: list) -> tuple:
        if not query or not choices: return None, 0.0
        results = []
        for scorer in (fuzz.token_sort_ratio, fuzz.token_set_ratio):
            r = process.extractOne(query, choices, scorer=scorer, processor=None)
            if r: results.append((r[0], float(r[1])))
        return max(results, key=lambda x: x[1]) if results else (None, 0.0)

    def _top_fuzzy(self, query: str, choices: list, limit: int = 5) -> list[tuple[str, float]]:
        if not query or not choices or limit <= 0:
            return []
        scores: dict[str, float] = {}
        for scorer in (fuzz.token_sort_ratio, fuzz.token_set_ratio):
            for c, s, _ in process.extract(query, choices, scorer=scorer, processor=None, limit=limit):
                s = float(s)
                if c not in scores or s > scores[c]:
                    scores[c] = s
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:limit]

    def suggest_states(self, raw_state: str, limit: int = 5) -> list[dict]:
        if is_blank(raw_state):
            return []
        norm = normalize_text(raw_state, self.stop_words)
        if not norm:
            return []
        query = normalize_text(self.state_aliases.get(norm, norm), self.stop_words)
        out = []
        for choice, score in self._top_fuzzy(query, self.state_choices, limit=limit):
            rec = self.state_norm_map.get(choice)
            if not rec:
                continue
            out.append({
                "state_lgd_code": rec["state_lgd_code"],
                "state_name": rec["state_name"],
                "score": round(score, 2),
                "status": self._status(score),
            })
        return out

    def suggest_districts(self, raw_district: str, state_lgd_code: str | None = None, limit: int = 5) -> list[dict]:
        if is_blank(raw_district):
            return []
        norm = normalize_text(raw_district, self.stop_words)
        if not norm:
            return []
        query = normalize_text(self.district_aliases.get(norm, norm), self.stop_words)
        sc = "" if is_blank(state_lgd_code) else str(state_lgd_code).strip()
        norm_map = self.district_norm_by_state.get(sc, {}) if sc else self.global_district_norm_map
        choices = self.district_choices_by_state.get(sc, []) if sc else self.global_district_choices
        global_mode = not bool(sc)

        out = []
        for choice, score in self._top_fuzzy(query, choices, limit=limit):
            cand = norm_map.get(choice)
            if cand is None:
                continue
            if global_mode:
                for rec in cand:
                    out.append({
                        "district_lgd_code": rec["district_lgd_code"],
                        "district_name": rec["district_name"],
                        "state_lgd_code": rec.get("state_lgd_code"),
                        "score": round(score, 2),
                        "status": self._status(score),
                    })
            else:
                out.append({
                    "district_lgd_code": cand["district_lgd_code"],
                    "district_name": cand["district_name"],
                    "state_lgd_code": cand.get("state_lgd_code"),
                    "score": round(score, 2),
                    "status": self._status(score),
                })

        seen = set()
        deduped = []
        for r in sorted(out, key=lambda x: x["score"], reverse=True):
            k = (r.get("state_lgd_code"), r.get("district_lgd_code"))
            if k in seen:
                continue
            seen.add(k)
            deduped.append(r)
            if len(deduped) >= limit:
                break
        return deduped

    @lru_cache(maxsize=50000)
    def match_state(self, raw_state: str) -> dict:
        empty = {"state_lgd_code": None, "state_name_corrected": None,
                 "state_score": 0.0, "state_status": "NOT_FOUND"}
        if is_blank(raw_state): return empty
        raw = str(raw_state).strip()
        if raw.lower() in self.state_exact_map:
            m = self.state_exact_map[raw.lower()]
            return {"state_lgd_code": m["state_lgd_code"], "state_name_corrected": m["state_name"],
                    "state_score": 100.0, "state_status": "EXACT"}
        norm = normalize_text(raw, self.stop_words)
        if not norm: return empty
        if norm in self.state_norm_map:
            m = self.state_norm_map[norm]
            return {"state_lgd_code": m["state_lgd_code"], "state_name_corrected": m["state_name"],
                    "state_score": 100.0, "state_status": "EXACT"}
        query = norm
        alias_val = self.state_aliases.get(norm)
        if alias_val:
            alias_norm = normalize_text(alias_val, self.stop_words)
            if alias_norm in self.state_norm_map:
                m = self.state_norm_map[alias_norm]
                return {"state_lgd_code": m["state_lgd_code"], "state_name_corrected": m["state_name"],
                        "state_score": 100.0, "state_status": "EXACT"}
            query = alias_norm
        choice, score = self._best_fuzzy(query, self.state_choices)
        if choice is None or score < self.thresholds["low_confidence"]: return empty
        m = self.state_norm_map[choice]
        return {"state_lgd_code": m["state_lgd_code"], "state_name_corrected": m["state_name"],
                "state_score": round(score, 2), "state_status": self._status(score)}

    @lru_cache(maxsize=200000)
    def match_district(self, raw_district: str, state_lgd_code: str) -> dict:
        empty = {"district_lgd_code": None, "district_name_corrected": None,
                 "district_score": 0.0, "district_status": "NOT_FOUND"}
        if is_blank(raw_district): return empty
        raw = str(raw_district).strip()
        raw_k = raw.lower()
        norm = normalize_text(raw, self.stop_words)
        if not norm: return empty
        sc = "" if is_blank(state_lgd_code) else str(state_lgd_code).strip()
        _debug_log(
            "H4",
            "matcher.py:match_district_entry",
            "District match called",
            {
                "raw_district": raw,
                "normalized_district": norm,
                "state_lgd_code_in": state_lgd_code,
                "state_lgd_code_normalized": sc,
                "global_mode": (not bool(sc)),
            },
        )

        exact_map = self.district_exact_by_state.get(sc, {}) if sc else self.global_district_exact_map
        norm_map  = self.district_norm_by_state.get(sc, {})  if sc else self.global_district_norm_map
        choices   = self.district_choices_by_state.get(sc, []) if sc else self.global_district_choices
        global_mode = not bool(sc)

        def _get(mapping, key):
            v = mapping.get(key)
            if v is None: return None
            if global_mode:
                if len(v) == 1: return v[0]
                logger.warning("Ambiguous district '%s' across states; skipping.", raw_district)
                return None
            return v

        if cand := _get(exact_map, raw_k):
            return {"district_lgd_code": cand["district_lgd_code"], "district_name_corrected": cand["district_name"],
                    "district_score": 100.0, "district_status": "EXACT"}
        if cand := _get(norm_map, norm):
            return {"district_lgd_code": cand["district_lgd_code"], "district_name_corrected": cand["district_name"],
                    "district_score": 100.0, "district_status": "EXACT"}

        query = norm
        alias_val = self.district_aliases.get(norm)
        if alias_val:
            alias_norm = normalize_text(alias_val, self.stop_words)
            if cand := _get(norm_map, alias_norm):
                return {"district_lgd_code": cand["district_lgd_code"], "district_name_corrected": cand["district_name"],
                        "district_score": 100.0, "district_status": "EXACT"}
            query = alias_norm

        # If state is unknown, avoid "guessing" districts via fuzzy search.
        # Use suggest_districts() to guide the user instead.
        if global_mode:
            _debug_log(
                "H5",
                "matcher.py:match_district_global_mode_return",
                "Global mode returns NOT_FOUND without fuzzy matching",
                {"raw_district": raw, "normalized_district": norm},
            )
            return empty

        choice, score = self._best_fuzzy(query, choices)
        if choice is None or score < self.thresholds["low_confidence"]: return empty
        if cand := _get(norm_map, choice):
            _debug_log(
                "H4",
                "matcher.py:match_district_success",
                "District fuzzy match succeeded",
                {
                    "raw_district": raw,
                    "state_lgd_code": sc,
                    "choice": choice,
                    "score": score,
                    "district_lgd_code": cand.get("district_lgd_code"),
                },
            )
            return {"district_lgd_code": cand["district_lgd_code"], "district_name_corrected": cand["district_name"],
                    "district_score": round(score, 2), "district_status": self._status(score)}
        return empty

    def match_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        data = df.copy()
        if "id" not in data.columns:
            data.insert(0, "id", range(1, len(data) + 1))
        for col in ["state_name_raw", "district_name_raw"]:
            if col not in data.columns:
                data[col] = ""
        data["state_name_raw"] = data["state_name_raw"].fillna("").astype(str)
        data["district_name_raw"] = data["district_name_raw"].fillna("").astype(str)
        logger.info("Matching %d rows...", len(data))

        unique_states = data["state_name_raw"].unique().tolist()
        state_cache = {s: self.match_state(s) for s in unique_states}
        data["_sm"] = data["state_name_raw"].map(state_cache)
        data["state_lgd_code"]      = data["_sm"].map(lambda x: x["state_lgd_code"])
        data["state_name_corrected"] = data["_sm"].map(lambda x: x["state_name_corrected"])
        data["_ss"]  = data["_sm"].map(lambda x: x["state_score"])
        data["_sst"] = data["_sm"].map(lambda x: x["state_status"])

        pairs = data[["district_name_raw", "state_lgd_code"]].drop_duplicates()
        dist_cache = {}
        for _, r in pairs.iterrows():
            key = (str(r["district_name_raw"]), "" if is_blank(r["state_lgd_code"]) else str(r["state_lgd_code"]))
            dist_cache[key] = self.match_district(*key)

        data["_dk"] = list(zip(data["district_name_raw"], data["state_lgd_code"].fillna("").astype(str)))
        data["_dm"] = data["_dk"].map(dist_cache)
        data["district_lgd_code"]       = data["_dm"].map(lambda x: x["district_lgd_code"])
        data["district_name_corrected"] = data["_dm"].map(lambda x: x["district_name_corrected"])
        data["_ds"]  = data["_dm"].map(lambda x: x["district_score"])
        data["_dst"] = data["_dm"].map(lambda x: x["district_status"])

        data["match_confidence_score"] = (data["_ss"] * 0.4 + data["_ds"] * 0.6).round(2)

        def final_status(row):
            if row["_sst"] == "NOT_FOUND" or row["_dst"] == "NOT_FOUND": return "NOT_FOUND"
            if row["_sst"] == "EXACT" and row["_dst"] == "EXACT": return "EXACT"
            s = row["match_confidence_score"]
            t = self.thresholds
            if s >= t["high_confidence"]: return "HIGH_CONFIDENCE"
            if s >= t["medium_confidence"]: return "MEDIUM_CONFIDENCE"
            if s >= t["low_confidence"]: return "LOW_CONFIDENCE"
            return "NOT_FOUND"

        data["match_status"] = data.apply(final_status, axis=1)
        out = ["id","state_name_raw","district_name_raw","state_lgd_code",
               "state_name_corrected","district_lgd_code","district_name_corrected",
               "match_confidence_score","match_status"]
        return data[out].copy()
