import os
import sys
import uvicorn

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests.mock_posting_server import app

if __name__ == "__main__":
    port = int(os.environ.get("MOCK_POSTING_SERVICE_PORT", "8085"))
    print(f"Starting Mock Posting Service on http://127.0.0.1:{port}...")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
