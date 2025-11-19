from pathlib import Path
import csv

CONFIG_DIR = Path("config")
PROJECT_PAY_RATE_OVERRIDES_FILE = CONFIG_DIR / "project_pay_rate_overrides.csv"



def load_project_pay_rate_overrides(filepath: Path | None = None) -> list[dict]:
    """
    Lead the project pay rate overrides CSV into a list of dictionaries.
    Each dict is one override rule (keys are taken from headers).
    If filepath is not provided, it defaults to config/project_pay_rate_overrides.csv
    """
    if filepath is None:
        filepath = PROJECT_PAY_RATE_OVERRIDES_FILE
   
   overrides: list[dict] = []
   
    with filepath.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            if not any(row.value()):
                continue
                
            clean_row = {
                key: (value.strip() if isinstance(value, str) else value)
                for key, value in row.items()
            }
            
            overrides.append(clean_row)
        
        return overrides
    
 
