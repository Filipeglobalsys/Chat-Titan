import json
from pathlib import Path

_CONFIG_FILE = Path(__file__).parent / "dataset_configs.json"
_configs: dict[str, dict] = {}


def _load_from_file():
    global _configs
    if _CONFIG_FILE.exists():
        try:
            _configs = json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            _configs = {}


def load_from_supabase():
    """Called explicitly at startup (after env vars are loaded) to merge Supabase configs."""
    try:
        from database import get_db
        db = get_db()
        res = db.table("datasets").select("id, dataset_config").execute()
        for row in (res.data or []):
            cfg = row.get("dataset_config")
            if cfg and isinstance(cfg, dict):
                _configs[row["id"]] = cfg
    except Exception:
        pass


def _persist(dataset_id: str, config: dict):
    # Persist to Supabase
    try:
        from database import get_db
        db = get_db()
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


_load_from_file()
