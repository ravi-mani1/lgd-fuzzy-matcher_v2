# LGD Fuzzy Matcher

Production-grade fuzzy matching system for Indian Government LGD data.

## Files
- `main.py` - CLI entry point
- `matcher.py` - Core matching engine
- `utils.py` - Utility helpers
- `app.py` - Streamlit web UI
- `api.py` - FastAPI REST service
- `config.json` - Aliases, thresholds, stop words
- `requirements.txt` - Python dependencies
- `sample_input.csv` - Sample input file

## Setup
```bash
pip install -r requirements.txt
# Copy lgd_STATE.csv and DISTRICT_STATE.csv into this folder
```

## Usage

### CLI
```bash
python main.py --input your_data.csv --output results.csv --sql updates.sql
```

### Streamlit UI
```bash
streamlit run app.py
```

### FastAPI
```bash
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

## Input Format
Columns: `id` (optional), `state_name_raw`, `district_name_raw`

## Output Columns
`state_lgd_code`, `state_name_corrected`, `district_lgd_code`, `district_name_corrected`, `match_confidence_score`, `match_status`

## match_status values
- `EXACT` - Perfect match
- `HIGH_CONFIDENCE` - Score >= 90
- `MEDIUM_CONFIDENCE` - Score 75-89
- `LOW_CONFIDENCE` - Score 60-74
- `NOT_FOUND` - Score < 60
