# HIHR Operations Analytics Dashboard

Internal project, rebuilding old pipeline/process better:

- Pull time, payroll, and invoicing data from Clockify, Paycor, and QuickBooks
- Transform it into a unified analytics model (employees, clients, projects)
- Power an interactive dashboard to show cost, revenue, and margin by client, employee, and project.

Tech stack: Python, DuckDB/Parquet, and a BI dashboard, plus maybe a Python-based dashboard (e.g. Streamlit) for new experience


Progress so far:
- Clockify can authenticate with API key from .env, list workspaces and projects, 
	pull time entires for a single user and date range
- Raw timeclock data is saved in data/raw/clockify/