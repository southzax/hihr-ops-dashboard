from pathlib import Path
import pandas as pd

BASE = Path("demo_data/processed")
CLOCKIFY_DIR = BASE / "clockify"
UNIFIED_DIR = BASE / "unified"

CLOCKIFY_DIR.mkdir(parents=True, exist_ok=True)
UNIFIED_DIR.mkdir(parents=True, exist_ok=True)

dim_users = pd.DataFrame(
    [
        {"user_id": "clk-user-1", "user_name": "Consultant A", "user_email": "a@example.com", "user_status": "ACTIVE"},
        {"user_id": "clk-user-2", "user_name": "Consultant B", "user_email": "b@example.com", "user_status": "ACTIVE"},
    ]
)

dim_projects = pd.DataFrame(
    [
        {"project_id": "clk-proj-1", "project_name": "HR Health Check", "client_name": "Client Alpha", "project_archived": False},
        {"project_id": "clk-proj-2", "project_name": "Onboarding Revamp", "client_name": "Client Beta", "project_archived": False},
    ]
)

time_entries = pd.DataFrame(
    [
        {
            "id": "te-001",
            "user_id": "clk-user-1",
            "project_id": "clk-proj-1",
            "workspace_id": "demo-workspace",
            "description": "HR strategy session",
            "billable": True,
            "tag": ["tag-billable"],
            "start": "2025-01-10T14:00:00Z",
            "end": "2025-01-10T16:00:00Z",
            "duration_raw": "PT2H",
            "duration_hours": 2.0,
        },
        {
            "id": "te-002",
            "user_id": "clk-user-1",
            "project_id": "clk-proj-2",
            "workspace_id": "demo-workspace",
            "description": "Internal planning",
            "billable": False,
            "tag": ["tag-internal"],
            "start": "2025-01-11T13:00:00Z",
            "end": "2025-01-11T14:30:00Z",
            "duration_raw": "PT1H30M",
            "duration_hours": 1.5,
        },
        {
            "id": "te-003",
            "user_id": "clk-user-2",
            "project_id": "clk-proj-1",
            "workspace_id": "demo-workspace",
            "description": "Client training",
            "billable": True,
            "tag": [],
            "start": "2025-01-12T15:00:00Z",
            "end": "2025-01-12T17:15:00Z",
            "duration_raw": "PT2H15M",
            "duration_hours": 2.25,
        },
    ]
)


fact_time_costed = time_entries.copy()
fact_time_costed["paycor_emp_id"] = fact_time_costed["user_id"].map(
    {
        "clk-user-1": "pc-emp-1",
        "clk-user-2": "pc-emp-2",
    }
)


hourly_rates = {
    "clk-user-1": 50.0,
    "clk-user-2": 70.0,
}

fact_time_costed["hourly_rate"] = fact_time_costed["user_id"].map(hourly_rates)
fact_time_costed["cost"] = fact_time_costed["duration_hours"] * fact_time_costed["hourly_rate"]


dim_users.to_parquet(CLOCKIFY_DIR / "dim_users.parquet", index=False)
dim_projects.to_parquet(CLOCKIFY_DIR / "dim_projects.parquet", index=False)
time_entries.to_parquet(CLOCKIFY_DIR / "time_entries_demo.parquet", index=False)

fact_time_costed.to_parquet(UNIFIED_DIR / "fact_time_costed.parquet", index=False)

print("Demo data written to data_demo/processed/")
