from pathlib import Path
import json
from typing import Any, List


RAW_CLOCKIFY_DIR = Path("data/raw/clockify")
RAW_PAYCOR_DIR = Path("data/raw/paycor")
PROCESSED_PAYCOR_DIR = Path("data/processed/paycor")


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
    
    
    
    
def ensure_raw_paycor_dir() -> None:
    """Make sure the raw Paycor data directory exists"""
    RAW_PAYCOR_DIR.mkdir(parents=True, exist_ok=True)

    
def save_paycor_employees_raw(employees: dict) -> Path:
    """
    Save raw Paycor employee data to a JSON file.
    -employees_response: the dict returned by get_employees_identifying_data
    """
    ensure_raw_paycor_dir()
    
    filepath = RAW_PAYCOR_DIR / "employee_identifying_info.json"
    
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(employees, f, indent=2, ensure_ascii=False)
        
    return filepath
    
    
    
def save_paycor_payrates_raw(payrates_by_employee: dict) -> Path:
    """
    Save raw payrate history for all employees.
    payrates_by_employee: {employeeId: [payrate records]} dictionary
    """
    ensure_raw_paycor_dir()

    filepath = RAW_PAYCOR_DIR / "payrates_all_employees.json"
    
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(payrates_by_employee, f, indent=2, ensure_ascii=False)

    return filepath


def save_paycor_payruns_raw(payruns: dict) -> Path:
    """Save raw Paycor payrun data to a JSON file."""
    ensure_raw_clockify_dir()
    filepath = RAW_PAYCOR_DIR / "payruns.json"

    with filepath.open("w", encoding="utf-8") as f:
        json.dump(payruns, f, indent=2, ensure_ascii=False)

    return filepath


def save_employee_earnings(earnings: dict) -> Path:
    """Save raw Paycor payrun data to a JSON file."""
    ensure_raw_clockify_dir()
    filepath = RAW_PAYCOR_DIR / "earnings.json"

    with filepath.open("w", encoding="utf-8") as f:
        json.dump(payruns, f, indent=2, ensure_ascii=False)

    return filepath



def ensure_processed_paycor_dir():
    """Make sure the processed Paycor data directory exists"""
    PROCESSED_PAYCOR_DIR.mkdir(parents=True, exist_ok=True)


def save_paycor_file_processed(df, name: str) -> None:
    """Save processed paycor file as CSV and Parquet"""
    ensure_processed_paycor_dir()
    csv_path = PROCESSED_PAYCOR_DIR / f"{name}.csv"
    parquet_path = PROCESSED_PAYCOR_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)


    
    
    
    
    
    
    