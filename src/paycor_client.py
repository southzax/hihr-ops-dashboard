import os
from dotenv import load_dotenv
import requests

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
    
    return {
        "client_id": PAYCOR_CLIENT_ID,
        "company_id": PAYCOR_COMPANY_ID,
        "client_secret": PAYCOR_CLIENT_SECRET
    }
    


def get_paycor_headers(access_token: str) -> dict:
    """Construct the headers required by the Paycor API."""
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }


    
def get_access_token() -> str:
    """
    DELETE LATER?
    This one didn't quite work--I don't think we need this one anymore, but leaving here for now.
    Request an OAuth access token from Paycor using client credentials.
    """
    
    creds = get_paycor_credentials()
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]
    
    token_url = os.getenv("PAYCOR_TOKEN_URL")
    if not token_url:
        raise RuntimeError("Paycor token url not set in .env file")
     
    data = {
        "grant_type":  "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
            
    scope = os.getenv("PAYCOR_SCOPE")
    subscription_key = os.getenv("PAYCOR_APIM_SUBSCRIPTION_KEY")
    if not subscription_key:
        raise RuntimeError("PAYCOR_APIM_SUBSCRIPTION_KEY not set in environment or .env")
   
    if scope:
        data["scope"] = scope
        
    headers = {
        "Ocp-Apim-Subscription-Key": subscription_key,
    }
        
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    
    token_data = response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError(
            f"Token endpoint did not return access_token.  Response was: {token_data}"
        )
            
    return access_token
    
    
    
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
    
    subscription_key = os.getenv("PAYCOR_APIM_SUBSCRIPTION_KEY")
    if not subscription_key:
        raise RuntimeError(
            "PAYCOR_APIM_SUBSCRIPTION_KEY not set in .env"
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
    
    headers = {
        "Ocp-Apim-Subscription-Key": subscription_key,
    }
    
    response = requests.post(token_url, data=data, headers=headers)
    
    """DETELE THIS LATER, JUST FOR DEBUGGING"""
    if response.status_code >= 400:
        print("Token endpoint returned status:", response.status_code)
        print("Raw response text:", response.text)

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
    response.raise_for_status()
        
    return response.json()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    