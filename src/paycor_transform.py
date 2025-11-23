from pathlib import Path
import json
import pandas as pd
from storage import save_paycor_file_processed

"""
This file pulls the raw data files from /data/raw/paycor and better-ify-s them.
Currently, it puts the data into a DataFrame, generates and merges dimension tables for
    all users and projects, and then saves the data as CSV and Parquet to
    data/processed/paycor
"""

RAW_PAYCOR_DIR = Path("data/raw/paycor")
PROCESSED_PAYCOR_DIR = Path("data/processed/paycor")



def load_paycor_file(filepath: Path) -> dict:
    """Load raw pay data JSON from disk into a Python list."""
    with filepath.open("r", encoding="utf-8") as f:
        return json.load(f)
"""
The function above makes sense to be here, but it's identical to a function  in
clockify_transform.py -- it doesn't make sense to put it somewhere else . . . but it feels
wasteful to have two identical functions.  Should I be importing it?
"""
        
        
def flatten_payrates(raw: dict) -> list[dict]:
    flat_data: list[dict] = []

    for emp_id, info in raw.items():
        records = info.get("records") or []
        for rate in records:
            if not isinstance(rate, dict):
                continue

            row = rate.copy()
            row["emp_id"] = emp_id
            flat_data.append(row)

    return flat_data
            
        
        
def to_pay_rate_dataframe(payrates: list) -> pd.DataFrame:
    """Convert a list of pay dicts to DataFrame with important keys."""
    if not payrates:
        return pd.DataFrame()
        
    records: list[dict] = []
    
    for pr in payrates:
        record = {
            "id": pr.get("id"),
            "start_date": pr.get("effectiveStartDate"),
            "end_date": pr.get("effectiveEndDate"),
            "number": pr.get("sequenceNumber"),
            "rate": pr.get("payRate"),
            "salary": pr.get("annualPayRate"),
            "description": pr.get("description"),
            "type": pr.get("type"),
            "reason": pr.get("reason"),
            "notes": pr.get("notes"),
            "emp_id": pr.get("emp_id"),
        }
        records.append(record)
        
    df = pd.DataFrame(records)
    
    if "start_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    if "end_date" in df.columns:
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    return df


def paycor_payrates_to_dataframe(raw_filepath: Path) -> pd.DataFrame:
    """Loads JSON, flattens structure and builds Dataframe."""
    raw_files = load_paycor_file(raw_filepath)
    flat = flatten_payrates(raw_files)
    df = to_pay_rate_dataframe(flat)
    return df


def employees_to_dataframe(raw: dict) -> pd.DataFrame:
    """Convert raw Paycor employee response to dim table."""
    records = raw.get("records") or []
    cleaned: list[dict] = []

    for emp in records:
        row = {
            "emp_id": emp.get("employeeId"),
            "first_name": emp.get("firstName"),
            "last_name": emp.get("lastName"),
            "status": emp.get("status"),
        }
        cleaned.append(row)

    df = pd.DataFrame(cleaned)

    return df


def build_fact_payrate_history() -> None:
    """Build and save the pay rate history table from raw Paycor JSON."""
    raw_path = RAW_PAYCOR_DIR / "payrates_all_employees.json"
    raw_files = load_paycor_file(raw_path)
    flat = flatten_payrates(raw_files)
    df_rates = to_pay_rate_dataframe(flat)
    save_paycor_file_processed(df_rates, name="payrate_history")
    print("Saved processed pay rate history to data/processed/paycor/payrate_history.*")


def build_dim_employee() -> None:
    """Build and save the employee dimension table from raw Paycor JSON."""
    raw_path = RAW_PAYCOR_DIR / "employee_identifying_info.json"
    raw_employees = load_paycor_file(raw_path)
    df_employees = employees_to_dataframe(raw_employees)
    save_paycor_file_processed(df_employees, name="employees_dim")
    print("Saved dim_employee to data/processed/paycor/employees_dim.*")





if __name__ == "__main__":
    raw_rates_path = RAW_PAYCOR_DIR / "payrates_all_employees.json"
    raw_emps_path = RAW_PAYCOR_DIR / "employee_identifying_info.json"
    raw_employees = load_paycor_file(raw_emps_path)
    df_rates = paycor_payrates_to_dataframe(raw_rates_path)
    df_employees = employees_to_dataframe(raw_employees)
    name = f"payrates_all_employees_{pd.Timestamp.today().date()}"
    save_paycor_file_processed(df_rates, "payrate_history")
    save_paycor_file_processed(df_employees, "employees_dim")

    build_dim_employee()
    build_fact_payrate_history()


