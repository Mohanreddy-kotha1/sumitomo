from pathlib import Path
import yaml

def project_root() -> Path:
    # framework/ -> repo root
    return Path(__file__).resolve().parents[1]

def load_env_config(env: str) -> dict:
    cfg_path = project_root() / "conf" / "environments" / f"{env}.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def resolve_path(env_cfg: dict, key: str, dataset: str | None = None):
    """
    Returns an absolute local path for local env, or S3 URI if value starts with s3://.
    If dataset is provided, append it as a child.
    """
    base = env_cfg["paths"][key]
    if isinstance(base, dict) and dataset:
        base = base.get(dataset, base.get("default"))
    if str(base).startswith("s3://"):
        return f"{base.rstrip('/')}/{dataset}" if dataset else base
    p = Path(base)
    if not p.is_absolute():
        p = project_root() / p
    return (p / dataset) if dataset else p
