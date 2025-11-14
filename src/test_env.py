from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Grab your test variable
clockify_key = os.getenv("CLOCKIFY_API_KEY")

print("CLOCKIFY_API_KEY =", clockify_key)