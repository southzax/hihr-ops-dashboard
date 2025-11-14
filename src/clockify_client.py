import os
from dotenv import load_dotenv
import requests
from datetime import datetime
from storage import save_time_entries_raw

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
    
    

def get_time_entries_for_user(
    workspace_id: str,
    user_id: str,
    start: str,
    end: str,
    page_size: int = 1000,
) -> list:
    """
    Fetch time entries for a single user.
    Uses endpoint:  GET /workspaces/{workspaceId}/user/{userId}/time-entries
    """
    url = f"{BASE_URL}/workspaces/{workspace_id}/user/{user_id}/time-entries"
    
    params = {
        "start": start,
        "end": end,
        "page-size": page_size
    }
    
    response = requests.get(url, headers=get_headers(), params=params)
    response.raise_for_status()
    return response.json()


    
if __name__ == "__main__":
    try:
        user = get_user()
        print("User:", user.get("name"), "-", user.get("email"))
        user_id = user.get("id")
        
        workspaces = get_workspaces()
        print("\nWorkspaces found:")
        for ws in workspaces:
            print(f"-{ws.get('id')} | {ws.get('name')}")
            
        workspace_id = get_workspace_id()
        print(f"\nUsing workspace: {workspace_id}")
        
        projects = get_projects(workspace_id)
        print(f"\nProjects found in workspace ({len(projects)} total):")
        for p in projects[:10]:
            print(f"- {p.get('id')} | {p.get('name')}")   
            
        print("\nTime Entries Sample (for current user):")
        start = "2025-01-01T00:00:00Z"
        end = "2025-01-31T23:59:59Z"
        
        time_entries = get_time_entries_for_user(workspace_id, user_id, start, end)
        print(f"Fetched {len(time_entries)} time entries.")
        
        filepath = save_time_entries_raw(time_entries, workspace_id, start, end)
        print(f"Raw time entries saved to: {filepath}")
        
        if not time_entries:
            print("No time entries found for this period.")
        else:    
            print("\nSample time entry (raw keys):")
            te = time_entries[0]
            print(list(te.keys()))
                
            ti = te.get("timeInterval", {}) or {}
            print("\nTime interval details:")
            print("Start:", ti.get("start"))
            print("End:", ti.get("end"))
            print("Duration:", ti.get("duration"))
            
            print("\nOther useful fields:")
            print("Description:", te.get("description"))
            print("User ID:", te.get("userId"))
            print("Project ID:", te.get("projectId"))
            print("Tag IDs:", te.get("tagIds"))
            
    except Exception as e:
        print("Unexpected error:", repr(e), "of type", type(e))
        