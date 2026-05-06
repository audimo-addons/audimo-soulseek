"""Entry point for the PyInstaller binary."""
import os
import sys
import uvicorn

PORT = int(os.environ.get("AUDIMO_ADDON_PORT", "9008"))

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=PORT,
        proxy_headers=True,
        access_log=False,
    )
