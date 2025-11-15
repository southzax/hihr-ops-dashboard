from pathlib import Path
import json
import pandas as pd

RAW_CLOCKIFY_DIR = Path("data/raw/clockify")
PROCESSED_CLOCKIFY_DIR = Path("data/processed/clockify")

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
    


def ensure_processed_clockify_dir() -> None:
    """Make sure the processed Clockify data directory exists."""
    PROCESSED_CLOCKIFY_DIR.mkdir(parents=True, exist_ok=True)
    
    
    
def save_time_entries_processed(
    df: pd.DataFrame,
    workspace_id: str,
    start: str,
    end: str
) -> dict:
    """Save the processed time entries DataFrame to disk (CSV and PArquet)."""
    ensure_processed_clockify_dir()
    
    start_date = start.split("T")[0]
    end_date = end.split("T")[0]
    base_name = f"time_entries_{workspace_id}_{start_date}_to_{end_date}"
    
    csv_path = PROCESSED_CLOCKIFY_DIR / f"{base_name}.csv"
    parquet_path = PROCESSED_CLOCKIFY_DIR / f"{base_name}.parquet"
    
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    
    return {"csv": csv_path, "parquet": parquet_path}




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
        
        if df.empty:
            print("\nNo data to save (DataFrame is empty).")
        else:
            name_parts = filepath.stem.split("_")
            workspace_id = name_parts[2]
            start_date = name_parts[3]
            end_date = name_parts[5]
        
            start = f"{start_date}T00:00:00Z"
            end = f"{end_date}T23:59:59Z"
            
            paths = save_time_entries_processed(df, workspace_id, start, end)
            print("\nProcessed data saved to:")
            print("CSV:", paths["csv"])
            print("Parquet:", paths["parquet"])        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    