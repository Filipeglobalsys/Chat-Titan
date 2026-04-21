import asyncio
import time
import httpx
import os

_token_cache: str | None = None
_token_expiry: float = 0
_lock = asyncio.Lock()


async def invalidate_token() -> None:
    global _token_cache, _token_expiry
    async with _lock:
        _token_cache = None
        _token_expiry = 0


async def get_access_token() -> str:
    global _token_cache, _token_expiry

    if _token_cache and time.time() < _token_expiry:
        return _token_cache

    async with _lock:
        # Re-check after acquiring lock (another coroutine may have refreshed already)
        if _token_cache and time.time() < _token_expiry:
            return _token_cache

        url = f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": os.getenv("CLIENT_ID"),
            "client_secret": os.getenv("CLIENT_SECRET"),
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(url, data=data)
            resp.raise_for_status()
            result = resp.json()

        _token_cache = result["access_token"]
        _token_expiry = time.time() + result["expires_in"] - 60
        return _token_cache
