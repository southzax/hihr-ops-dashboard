from pathlib import Path
import json
import pandas as pd

RAW_CLOCKIFY_DIR = Path("data/raw/clockify")

def load_raw_time_entries(filepath: Path) -> list:
    """Load raw time entry JSON from disk into a Python list."""
    with filepath.open("r", encoding="utf-8") as f:
        return json.load(f)
        
def to_time_entries_dataframe(entries: list) -> pd.DataFrame:
    """Convert a list of time dicts to DataFrame with important keys."""
    if not entries:
        return pd.DataFrame()
        
    records = []
    for te in entries:
        ti = te.get("timeInterval", {}) or {}
        record = {
            "id": te.get("id"),
            "user_id": te.get("userId"),
            "project_id": te.get("projectID"),
            "workspace_id": te.get("workspaceId"),
            "description": te.get("description"),
            "billable": te.get("billable"),
            "tag": te.get("tagIDs"),
            "start": ti.get("start"),
            "end": ti.get("end"),
            "duration_raw": ti.get("duration"),
        }
        
        records.append(record)
        
    df = pd.DataFrame(records)
    
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
    if "end" in df.columns:
        df["end"] = pd.to_datetime(df["end"], errors="coerce")
        
    def parse_duration_to_hours(d):
        """Compute duration_hours from duration_raw."""
        if not isinstance(d, str) or not d.startswith("PT"):
            return None
        hours = 0.0
        num = ""
        for ch in d[2:]:
            if ch.isdigit() or ch == ".":
                num += ch
            else:
                if num:
                    value = float(num)
                    if ch == "H":
                        hours += value
                    elif ch == "M":
                        hours += value / 60.0
                    elif ch == "S":
                        hours += value / 3600.0
                    num = ""
        return hours
    
    df["duration_hours"] = df["duration_raw"].apply(parse_duration_to_hours)

    return df
    


if __name__ == "__main__":
    #COME BACK TO THIS!!!  Just testing for now.
    raw_files = sorted(RAW_CLOCKIFY_DIR.glob("time_entries_*.json"))
    if not raw_files:
        print("No raw Clockify files found in data/raw/clockify.")
    else:
        filepath = raw_files[-1]
        print(f"Loading raw time entries from: {filepath}")    
        
        entries = load_raw_time_entries(filepath)
        print(f"Loaded {len(entries)} raw entries.")
        
        df = to_time_entries_dataframe(entries)
        print(f"DataFrame shape: {df.shape}")
        print("\nSample rows:")
        print(df.head())
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    