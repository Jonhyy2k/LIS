from google import genai
from google.genai import types
import os

os.environ["API_KEY"] = "AIzaSyBCFWQwe6vOxrQBgSDA9XcpGtzyFAZSWa0"
OUTPUT_FILE = "Forecasts.txt"

ticker = input("Enter a ticker: ").strip().upper()
model = "gemini-2.0-flash"

client = genai.Client(api_key=os.environ["API_KEY"])
response = client.models.generate_content(
    model=model,
    contents=f"Give me the revenue growth in % expectations average for {ticker} for 2026-2030, just provide the figures next to the years, nothing more"
)

try:
    # Extract the text from the response object
    response_text = response.text
    
    with open(OUTPUT_FILE, "a") as file:
        file.write(f"REVENUE FORECAST FOR {ticker} (using {model})\n")
        file.write("---------------------------------------------------\n")
        file.write(response_text + "\n")
        file.write("---------------------------------------------------\n")
        file.write(f"Analysis complete for {ticker}\n\n")
    
    print(f"Results for {ticker} appended to {OUTPUT_FILE}")
except IOError as e:
    print(f"Error writing to file {OUTPUT_FILE}: {e}")
except Exception as e:
    print(f"An unexpected error occurred during file operation: {e}")