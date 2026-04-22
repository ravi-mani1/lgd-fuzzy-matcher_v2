"""
api.py - FastAPI REST service for LGD Fuzzy Matcher
Run: uvicorn api:app --host 0.0.0.0 --port 8000 --reload
"""
import io, time
from typing import List, Optional
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from matcher import LGDMatcher
from utils import setup_logging, load_config

app = FastAPI(title="LGD Fuzzy Matcher API", version="1.0.0",
              description="Maps raw Indian state/district names to official LGD codes")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

cfg    = load_config("config.json")
logger = setup_logging(cfg.get("logging",{}).get("log_file","lgd_matcher.log"),
                       cfg.get("logging",{}).get("level","INFO"))
_matcher: Optional[LGDMatcher] = None


def get_matcher() -> LGDMatcher:
    global _matcher
    if _matcher is None:
        _matcher = LGDMatcher(config_path="config.json")
        _matcher.load_master_from_csv("lgd_STATE.csv", "DISTRICT_STATE.csv")
        logger.info("Matcher initialized.")
    return _matcher


class MatchRecord(BaseModel):
    id: Optional[str] = None
    state_name_raw: str   = Field(..., example="delhii")
    district_name_raw: str = Field(..., example="New Delhi")

class MatchRequest(BaseModel):
    records: List[MatchRecord]


@app.on_event("startup")
async def startup():
    get_matcher()

@app.get("/health")
def health():
    return {"status": "ok", "service": "lgd-fuzzy-matcher"}

@app.get("/stats")
def stats():
    m = get_matcher()
    return {"states": len(m.state_df), "districts": len(m.district_df),
            "thresholds": m.thresholds}

@app.post("/match")
def match_records(payload: MatchRequest):
    if not payload.records:
        raise HTTPException(400, "No records provided.")
    df = pd.DataFrame([r.model_dump() for r in payload.records], dtype=str)
    t0 = time.perf_counter()
    results = get_matcher().match_dataframe(df)
    elapsed = round(time.perf_counter() - t0, 3)
    results = results.where(pd.notnull(results), None)
    return {"total": len(results), "elapsed_sec": elapsed,
            "status_summary": results["match_status"].value_counts().to_dict(),
            "results": results.to_dict(orient="records")}

@app.post("/match-csv")
async def match_csv(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content), dtype=str)
    except Exception as e:
        raise HTTPException(400, f"Invalid CSV: {e}")
    missing = {"state_name_raw","district_name_raw"} - set(df.columns)
    if missing:
        raise HTTPException(422, f"Missing columns: {sorted(missing)}")
    t0 = time.perf_counter()
    results = get_matcher().match_dataframe(df)
    elapsed = round(time.perf_counter() - t0, 3)
    results = results.where(pd.notnull(results), None)
    return {"total": len(results), "elapsed_sec": elapsed,
            "status_summary": results["match_status"].value_counts().to_dict(),
            "results": results.to_dict(orient="records")}
