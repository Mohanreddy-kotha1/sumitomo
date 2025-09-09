from pathlib import Path
import os, yaml

def _read_yaml(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_env_config(env: str):
    path = Path(f"conf/environments/{env}.yaml")
    if not path.exists():
        raise FileNotFoundError(f"Environment config not found: {path}")
    return _read_yaml(path)

def load_table_config(table: str):
    path = Path(f"conf/tables/{table}.yaml")
    if not path.exists():
        raise FileNotFoundError(f"Table config not found: {path}")
    return _read_yaml(path)

def resolve_path(env_cfg: dict, key: str, dataset: str | None = None) -> str:
    base = env_cfg["paths"][key]  # e.g., bronze_delta
    if dataset:
        if base.startswith("s3://"):
            return f"{base}/{dataset}"
        return str(Path(base) / dataset)
    return base
