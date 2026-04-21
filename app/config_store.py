import json
from pathlib import Path

_CONFIG_FILE = Path(__file__).parent / "dataset_configs.json"
_configs: dict[str, dict] = {}


def _get_db():
    from database import get_db
    return get_db()


def _load():
    global _configs
    # Load from local file (development fallback)
    if _CONFIG_FILE.exists():
        try:
            _configs = json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            _configs = {}

    # Merge with Supabase (authoritative for production/Vercel)
    try:
        db = _get_db()
        res = db.table("datasets").select("id, dataset_config").not_.is_("dataset_config", "null").execute()
        for row in (res.data or []):
            if row.get("dataset_config"):
                _configs[row["id"]] = row["dataset_config"]
    except Exception:
        pass  # Supabase unavailable — continue with local data only


def _persist(dataset_id: str, config: dict):
    # Persist to Supabase
    try:
        db = _get_db()
        db.table("datasets").update({"dataset_config": config}).eq("id", dataset_id).execute()
    except Exception:
        pass

    # Also write local file (development convenience)
    try:
        _CONFIG_FILE.write_text(json.dumps(_configs, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass  # read-only filesystem (Vercel serverless)


def get_config(dataset_id: str) -> dict | None:
    return _configs.get(dataset_id)


def set_config(dataset_id: str, config: dict) -> None:
    _configs[dataset_id] = config
    _persist(dataset_id, config)


def get_all() -> dict:
    return dict(_configs)


_load()
