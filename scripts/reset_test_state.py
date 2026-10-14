import asyncio
import os
import sys
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.config import get_settings
from app.database.db import engine
from app.models.base import Base

settings = get_settings()


async def reset():
    print("Resetting local transaction database...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    print("Database reset complete.")

    mock_url = f"{settings.MOCK_POSTING_SERVICE_URL.rstrip('/')}/cleanup"
    print(f"Calling mock service cleanup at {mock_url}...")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(mock_url)
            print(f"Mock cleanup response: {resp.status_code} - {resp.text}")
    except Exception as exc:
        print(f"Could not reach mock service at {mock_url} ({exc}). Skipping remote cleanup.")


if __name__ == "__main__":
    asyncio.run(reset())
