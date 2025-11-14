import os
from dotenv import load_dotenv
import requests
from datetime import datetime

load_dotenv()

BASE_URL = "https://api.clockify.me/api/v1"

def get_api_key() -> str:
    """Read the Clockify API key from environment variables."""
    api_key = os.getenv("CLOCKIFY_API_KEY")
    if not api_key:
        raise RuntimeError("CLOCKIFY_API_KEY is not set in the environment or .env file.")
    return api_key
    
    
def get_workspace_id() -> str:
    """Read the Clockify Workspace ID from the environment variables."""
    workspace_id = os.getenv("CLOCKIFY_WORKSPACE_ID")
    if not workspace_id:
    	raise RuntimeError("CLOCKIFY_WORKSPACE_ID is not set in the environment or .env file")
    return workspace_id


def get_headers() -> dict:
    """Construct the headers required by the Clockify API."""
    return{
        "X-Api-Key": get_api_key()
    }
    
    
def get_user() -> dict:
    """Call a simple, safe Clockify endpoint that returns info about the current user."""
    url = f"{BASE_URL}/user"
    response = requests.get(url, headers=get_headers())
    response.raise_for_status()
    return response.json()
    
    
def get_workspaces() -> list:
    """
    Fetch all workspaces the authenticated user has access to.
    Returns a list of workspace objects.
    """
    url = f"{BASE_URL}/workspaces"
    response = requests.get(url, headers=get_headers())
    response.raise_for_status()
    return response.json()
    
    
def get_projects(workspace_id: str) -> list:
	"""
	Fetch all projects in a given workspace.
	Returns a list of project objects.
	"""
	url = f"{BASE_URL}/workspaces/{workspace_id}/projects"
	response = requests.get(url, headers=get_headers())
	response.raise_for_status()
	return response.json()
    
    
def get_time_entries(workspace_id: str, start: str, end: str) -> list:
    """
    Fetch all time entries in a workspace for a given date range.
    """
    url = f"{BASE_URL}/workspaces/{workspace_id}/time-entries"
    
    payload = {
        "start": start,
        "end": end,
        "hydrated": True
    }
    
    headers = get_headers()
    headers["Content-Type"] = "application/json"
    
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()
    
    
    
if __name__ == "__main__":
    try:
        user = get_user()
        print("User:", user.get("name"), "-", user.get("email"))
        
        workspaces = get_workspaces()
        print("\nWorkspaces found:")
        for ws in workspaces:
            print(f"-{ws.get('id')} | {ws.get('name')}")
            
        workspace_id = get_workspace_id()
        print(f"\nUsing workspace: {workspace_id}")
        
        projects = get_projects(workspace_id)
        print("\nProjects found in workspace ({len(projects)} total):")
        for p in projects[:10]:
            print(f"- {p.get('id')} | {p.get('name')}")   
            
        print("\nTime Entries Sample:")
        start = "2025-01-01T00:00:00Z"
        end = "2025-01-31T23:59:59Z"
        
        time_entries = get_time_entries(workspace_id, start, end)
        
        if time_entries:
            te = time_entries[0]
            print("- ID:", te.get("id"))
            print("- Description:", te.get("description"))
            print("- Project:", te.get("projectId"))
            print("- Start:", te.get("timeInterval", {}).get("start"))
            print("- End:", te.get("timeInterval", {}).get("start"))
        else:
            print("No time entries found for this period.")
        
    except Exception as e:
        print("Error:", e)
        