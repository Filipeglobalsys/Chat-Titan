from __future__ import annotations

from azure.identity import UsernamePasswordCredential
from azure.storage.blob.aio import BlobServiceClient


def _fmt_size(n: int | None) -> str:
    if n is None:
        return "N/A"
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n/1024:.1f} KB"
    if n < 1024 ** 3:
        return f"{n/1024**2:.1f} MB"
    return f"{n/1024**3:.2f} GB"


async def find_latest_parquet_blobs(
    *,
    tenant_id: str,
    client_id: str,
    username: str,
    password: str,
    storage_account: str,
    container_name: str,
    prefix: str = "",
    top_n: int = 20,
) -> list[dict]:
    credential = UsernamePasswordCredential(
        client_id=client_id,
        username=username,
        password=password,
        tenant_id=tenant_id,
    )

    account_url = f"https://{storage_account}.blob.core.windows.net"

    async with BlobServiceClient(account_url=account_url, credential=credential) as svc:
        container = svc.get_container_client(container_name)
        results: list[dict] = []
        async for blob in container.list_blobs(name_starts_with=prefix or None):
            if not blob.name.lower().endswith(".parquet"):
                continue
            results.append(
                {
                    "name": blob.name,
                    "last_modified": blob.last_modified.isoformat() if blob.last_modified else "",
                    "size_bytes": blob.size,
                    "size_readable": _fmt_size(blob.size),
                }
            )

    results.sort(key=lambda x: x["last_modified"], reverse=True)
    return results[:top_n]
