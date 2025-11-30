import os
from dotenv import load_dotenv
from typing import Any
import requests
from storage import save_paycor_employees_raw, save_paycor_payrates_raw, save_paycor_payruns_raw, save_employee_earnings
from datetime import datetime

load_dotenv()

def get_paycor_credentials() -> dict:
    """Read the Paycor credentials from environment variables and return a dict."""
    PAYCOR_CLIENT_ID = os.getenv("PAYCOR_CLIENT_ID")
    PAYCOR_COMPANY_ID = os.getenv("PAYCOR_COMPANY_ID")
    PAYCOR_CLIENT_SECRET = os.getenv("PAYCOR_CLIENT_SECRET")
    
    if not PAYCOR_CLIENT_ID or not PAYCOR_COMPANY_ID or not PAYCOR_CLIENT_SECRET:
        raise RuntimeError(
            "Paycor credentials not set in environment or .env file. "
            "Expected PAYCOR_CLIENT_ID, PAYCOR_COMPANY_ID, PAYCOR_CLIENT_SECRET"
        )
    
    credentials = {
        "client_id": PAYCOR_CLIENT_ID,
        "company_id": PAYCOR_COMPANY_ID,
        "client_secret": PAYCOR_CLIENT_SECRET
    }
    
    return credentials
    


def get_paycor_headers(access_token: str) -> dict:
    """Construct the headers required by the Paycor API."""
    subscription_key = os.getenv("PAYCOR_APIM_SUBSCRIPTION_KEY")
    if not subscription_key:
        raise RuntimeError(
            "PAYCOR_APIM_SUBSCRIPTION_KEY is not set in the envrionment or .env"
        )
        
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Content-Type": "application/json"
    }
    
    return headers

    
    
def get_access_token_from_refresh() -> str:
    """Use stored Paycor refresh token to request a new access token."""
    creds = get_paycor_credentials()
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]
    
    token_url = os.getenv("PAYCOR_TOKEN_URL")
    if not token_url:
        raise RuntimeError(
            "PAYCOR_TOKEN_URL not set in .env"
        )
    
    refresh_token = os.getenv("PAYCOR_REFRESH_TOKEN")
    if not refresh_token:
        raise RuntimeError(
            "PAYCOR_REFRESH_TOKEN not set in .env"
        )
    
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    
    response = requests.post(token_url, data=data)
    
    """DETELE THIS LATER, JUST FOR DEBUGGING
    if response.status_code >= 400:
        print("Token endpoint returned status:", response.status_code)
        print("Raw response text:", response.text)
    """

    response.raise_for_status()
    
    token_data = response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError(
            f"Token endpoint did not return an access_token.  Response was: {token_data}"
        )
    
    return access_token
    

    
def paycor_get(path: str, access_token: str, params: dict | None = None) -> dict:
    """
    Make a GET request to Paycor API using the relative path and access token.
    Return the parsed JSON response as a dict.
    """
    
    base_url = os.getenv("PAYCOR_BASE_URL")
    if not base_url:
        raise RuntimeError("PAYCOR_BASE_URL is not set in the environment or .env file.")
    base_url = base_url.rstrip("/")
    path = path.lstrip("/")
    request_url = f"{base_url}/{path}"
    
    headers = get_paycor_headers(access_token)
    
    response = requests.get(request_url, headers=headers, params=params)

    """Debugging helper, delete later:"""
    if not response.ok:
        print("Paycor GET error:", response.status_code, response.text)

    #response.raise_for_status()

    return response.json()

    

    
def get_employees_identifying_data(
    access_token: str,
    include_status: list[str] | None = None,
    continuation_token: str | None = None,
) -> dict[str, Any]:
    """
    Fetch employee data for all employees in the workspace.
    Uses Paycor's API endpoint /v1/legalentities/{legalEntityId}/employeesIdentifyingData
    """
    creds = get_paycor_credentials()
    legal_entity_id = creds["company_id"]
    
    path = f"v1/legalentities/{legal_entity_id}/employeesIdentifyingData"
    
    params: dict[str, Any] = {}
    if include_status:
        params["include"] = include_status
    if continuation_token:
        params["continuationToken"] = continuation_token
    
    emp_info =  paycor_get(path, access_token, params=params)
    
    return emp_info
    
    
    
def get_pay_data_for_user(
    access_token: str,
    employee_id: str,
    continuation_token: str | None = None,
) -> list:
    """
    Fetch pay rates for a single user.
    """
    creds = get_paycor_credentials()
    legal_entity_id = creds["company_id"]
    
    path = f"v1/employees/{employee_id}/payrates"
    
    user_pay_data = paycor_get(path, access_token)
   
    return user_pay_data


    
def get_pay_rates_for_all_users(
    access_token: str,
    employees: list[dict],
    continuation_token: str | None = None,
) -> dict:
    """
    Fetch pay rates for all users in a legal entity.
    Combines results into a unified list.
    """
    all_employee_rates = {}
    
    for e in employees:
        e_id = e["employeeId"]
        all_employee_rates[e_id] = get_pay_data_for_user(access_token, e_id)
        
    return all_employee_rates


"""WRONG API ENDPOINT for the fucntion below -- come back to this!!"""

def get_payruns(
        access_token: str,
        continuation_token: str | None = None,
) -> dict:
    """Returns payrun data for a date range."""

    creds = get_paycor_credentials()
    legal_entity_id = creds["company_id"]

    path = f"v1/legalentities/{legal_entity_id}/paydata"

    params = {
        "fromCheckDate": start,
        "toCheckDate": end,
    }

    if continuation_token:
        params["continuationToken"] = continuation_token

    payrun = paycor_get(path, access_token, params=params)

    return payrun


    
if __name__ == "__main__":
    access_token = get_access_token_from_refresh()

    #Get employees
    employees_response = get_employees_identifying_data(access_token)
    employees = employees_response["records"]

    #Save raw employee JSON
    save_paycor_employees_raw(employees_response)

    #Get payrate history for each employee
    payrates = get_pay_rates_for_all_users(access_token, employees)

    #Save raw payrate JSON
    save_paycor_payrates_raw(payrates)

    #Get raw payruns
    start = "2025-01-01T00:00:00Z"
    end = "2025-01-31T23:59:59Z"
    payruns = get_payruns(access_token)

    save_paycor_payruns_raw(payruns)

    print("Saved raw Paycor data → data/raw/paycor/")

    
    
    
    
    
    
    
    