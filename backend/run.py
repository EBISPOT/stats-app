import uvicorn
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

if __name__ == "__main__":
    # Development server configuration
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Enable auto-reload during development
        workers=1     # Number of worker processes
    )