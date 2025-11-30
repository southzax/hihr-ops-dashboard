from pathlib import Path
import pandas as pd


CLOCKIFY_PROCESSED_DIR = Path("data/processed/clockify")
PAYCOR_PROCESSED_DIR = Path("data/processed/paycor")
CONFIG_DIR = Path("config")
UNIFIED_DIR = Path("data/processed/unified")


def load_latest_time_entries() -> pd.DataFrame:
    """Load the most recent processed Clockify time entries file."""
    files = sorted(CLOCKIFY_PROCESSED_DIR.glob("time_entries_*.parquet"))
    if not files:
        raise FileNotFoundError("No processed Clockify time_entries parquet files found.")
    latest = files[-1]
    print(f"Loading time entries from {latest}")
    return pd.read_parquet(latest)


def load_dimensions() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load Clockify user and project dimensions (optional for future use)."""
    dim_users = pd.read_parquet(CLOCKIFY_PROCESSED_DIR / "dim_users.parquet")
    dim_projects = pd.read_parquet(CLOCKIFY_PROCESSED_DIR / "dim_projects.parquet")
    return dim_users, dim_projects


def load_payrate_history() -> pd.DataFrame:
    """Load Paycor pay rate history."""
    df = pd.read_parquet(PAYCOR_PROCESSED_DIR / "payrate_history.parquet")

    renames = {}
    if "rate" in df.columns and "hourly_rate" not in df.columns:
        renames["rate"] = "hourly_rate"
    if "emp_id" not in df.columns:
        pass

    if renames:
        df = df.rename(columns=renames)

    return df


def load_employee_id_mapping() -> pd.DataFrame:
    """
    Load mapping between Clockify users and Paycor employees.

    Expected columns in config/employee_id_mapping.csv:
    - clockify_user_id
    - paycor_emp_id
    """
    path = CONFIG_DIR / "employee_id_mapping.csv"
    if not path.exists():
        raise FileNotFoundError("Expected config/employee_id_mapping.csv for ID mapping.")
    mapping = pd.read_csv(path, dtype=str)
    return mapping


def attach_employee_ids(df_time: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """Add paycor_emp_id to time entries via mapping."""
    merged = df_time.merge(
        mapping,
        how="left",
        left_on="user_id",
        right_on="clockify_user_id",
    )
    return merged


def attach_pay_rates(df_time: pd.DataFrame, df_rates: pd.DataFrame) -> pd.DataFrame:
    """
    Attach applicable hourly_rate from pay rate history to each time entry,
    based on paycor_emp_id and entry start date.

    This is a simple, correct-first version, not heavily optimized.
    """

    df_time = df_time.copy()
    df_time["start"] = pd.to_datetime(df_time["start"], errors="coerce", utc="true")

    rates = df_rates.copy()
    rates["start_date"] = pd.to_datetime(rates["start_date"], errors="coerce", utc="true")

    if "end_date" in rates.columns:
        rates["end_date"] = pd.to_datetime(rates["end_date"], errors="coerce", utc="true")
        far_future = pd.Timestamp("2100-01-01", tz="UTC")
        rates["end_date"] = rates["end_date"].fillna(far_future)
    else:
        rates["end_date"] = pd.Timestamp("2100-01-01", tz="UTC")

    def find_rate_for_entry(row):
        emp_id = row.get("paycor_emp_id")
        entry_start = row.get("start")
        if pd.isna(emp_id) or pd.isna(entry_start):
            return None

        subset = rates[rates["emp_id"] == emp_id]
        if subset.empty:
            return None

        mask = (subset["start_date"] <= entry_start) & (entry_start <= subset["end_date"])
        subset = subset[mask]
        if subset.empty:
            return None

        best = subset.sort_values("start_date").iloc[-1]
        return best.get("hourly_rate")

    df_time["hourly_rate"] = df_time.apply(find_rate_for_entry, axis=1)
    return df_time


def compute_costs(df_time: pd.DataFrame) -> pd.DataFrame:
    """Compute cost per time entry and return a costed fact table."""
    df = df_time.copy()
    df["cost"] = df["duration_hours"] * df["hourly_rate"]

    cols = [
        "id",
        "user_id",
        "paycor_emp_id",
        "project_id",
        "start",
        "end",
        "duration_hours",
        "billable",
        "hourly_rate",
        "cost",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols]


def save_fact_time_costed(df: pd.DataFrame) -> None:
    """Save the combined costed time entries table to unified/."""
    UNIFIED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = UNIFIED_DIR / "fact_time_costed.csv"
    parquet_path = UNIFIED_DIR / "fact_time_costed.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print("Saved costed time entries to:")
    print("  CSV:    ", csv_path)
    print("  Parquet:", parquet_path)



if __name__ == "__main__":
    df_time_raw = load_latest_time_entries()
    dim_users, dim_projects = load_dimensions()
    df_rates = load_payrate_history()
    mapping = load_employee_id_mapping()

    df_time_with_ids = attach_employee_ids(df_time_raw, mapping)
    df_time_with_rates = attach_pay_rates(df_time_with_ids, df_rates)
    df_costed = compute_costs(df_time_with_rates)

    save_fact_time_costed(df_costed)

    print("\nSample of costed time entries:")
    print(df_costed.head())