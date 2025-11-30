# HIHR Operations Analytics Dashboard

Internal project, rebuilding old pipeline/process better:

- Pull time, payroll, and invoicing data from Clockify, Paycor, and QuickBooks
- Transform it into a unified analytics model (employees, clients, projects)
- Power an interactive dashboard to show cost, revenue, and margin by client, employee, and project.

Tech stack: Python, DuckDB/Parquet, and a BI dashboard, plus maybe a Python-based dashboard (e.g. Streamlit) for new experience


Progress so far:
- Clockify can authenticate with API key from .env, list workspaces and projects, 
	pull time entires for all users in a date range
- Raw timeclock data is saved in data/raw/clockify/
- Timeclock data is cleaned and transformed to DataFrame and dimension tables are
    built.  Transformed data is saved in csv and parquet formats in data/processed/clockify
- Paycor can authenticate through API app in Developer Portal.  Generates access token
    using refresh token from .env, lists employee identifying data and pay rate history
- Raw employee data and pay rate history is saved in data/raw/paycor/
- Payrate data is cleaned and transformed to DataFram and dimension tables are built.
    Transformed data is saved in csv and parquet formats in data/processed/paycor

TODO:
- Standardize module structure and function naming conventions across the ETL pipeline
    (clockify, paycor, quickbooks). After QuickBooks pipeline is implemented, refactor to ensure
    consistent separation of concerns (client calls, raw loaders, transforms, storage, etc.).
- Expand Paycor employee_identifying_info.csv to include inactive/terminated employees
- Configure override pay rates for special projects--add information for archived projects.