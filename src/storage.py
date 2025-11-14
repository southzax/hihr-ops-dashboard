from pathlib import Path
import json
from typing import Any, List


RAW_CLOCKIFY_DIR = Path("data/raw/clockify")


def ensure_raw_clockify_dir() -> None:
    """Make sure the raw Clockify data directory exists"""
    RAW_CLOCKIFY_DIR.mkdir(parents=True, exist_ok=True)
    
    
def save_time_entries_raw(
    entries: List[dict],
    workspace_id: str,
    start: str,
    end: str
) -> Path:
    """
    Save raw Clockify time entries to a JSON file.
    -entries: the list returned by get_time_entries()
    -workspace_id: which workspace the entries belong to
    -start,end: the original IS08601 timestamps
    """
    ensure_raw_clockify_dir()
    
    #Date range from ISO strings
    start_date = start.split("T")[0]
    end_date = end.split("T")[0]
    
    filename = f"time_entries_{workspace_id}_{start_date}_to_{end_date}.json"
    filepath = RAW_CLOCKIFY_DIR / filename
    
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False)
        
    return filepath