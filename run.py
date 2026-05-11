"""Entry point for the PyInstaller binary."""
import os
import sys
import uvicorn

PORT = int(os.environ.get("AUDIMO_ADDON_PORT", "9008"))

if __name__ == "__main__":
    # Default to 127.0.0.1; ``0.0.0.0`` requires an explicit opt-in.
    host = (
        os.getenv("AUDIMO_ADDON_HOST")
        or os.getenv("TUNNEL_ADDON_HOST")
        or "127.0.0.1"
    )
    uvicorn.run(
        "server:app",
        host=host,
        port=PORT,
        proxy_headers=True,
        access_log=False,
    )
