import json
from pathlib import Path

_CONFIG_FILE = Path(__file__).parent / "dataset_configs.json"
_configs: dict[str, dict] = {}


def _load():
    global _configs
    if _CONFIG_FILE.exists():
        try:
            _configs = json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            _configs = {}


def _persist():
    try:
        _CONFIG_FILE.write_text(json.dumps(_configs, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass  # read-only filesystem (e.g. Vercel serverless) — configs live in memory only


def get_config(dataset_id: str) -> dict | None:
    return _configs.get(dataset_id)


def set_config(dataset_id: str, config: dict) -> None:
    _configs[dataset_id] = config
    _persist()


def get_all() -> dict:
    return dict(_configs)


_load()
