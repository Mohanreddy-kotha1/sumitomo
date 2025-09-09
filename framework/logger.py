import json, logging, sys, time, io
from datetime import datetime
from pathlib import Path

class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "file": record.pathname,
            "line": record.lineno,
        }
        return json.dumps(payload, ensure_ascii=False)

def get_logger(name: str = "app", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(JsonFormatter())
        logger.addHandler(sh)
        logger.propagate = False
    return logger

def _timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def build_log_uri(log_dir, process_name: str, ts: str | None = None) -> str:
    ts = ts or _timestamp()
    fname = f"{process_name}__{ts}.log"
    # Respect local vs s3
    if str(log_dir).startswith("s3://"):
        return f"{str(log_dir).rstrip('/')}/{fname}"
    p = Path(log_dir)
    if not p.is_absolute():
        p = Path.cwd() / p
    p.mkdir(parents=True, exist_ok=True)
    return str(p / fname)

class S3BufferedHandler(logging.Handler):
    """Buffers JSON lines then writes once to s3:// on close."""
    def __init__(self, s3_uri: str):
        super().__init__()
        self.s3_uri = s3_uri
        self._buf = io.StringIO()

    def emit(self, record):
        try:
            msg = self.format(record)
            self._buf.write(msg + "\n")
        except Exception:  # pragma: no cover
            self.handleError(record)

    def close(self):
        try:
            import s3fs
            fs = s3fs.S3FileSystem(anon=False)
            with fs.open(self.s3_uri, "w") as f:
                f.write(self._buf.getvalue())
        finally:
            self._buf.close()
            super().close()

def add_log_destination(logger: logging.Logger, log_uri: str):
    fmt = JsonFormatter()
    if log_uri.startswith("s3://"):
        h = S3BufferedHandler(log_uri)
        h.setFormatter(fmt)
        logger.addHandler(h)
        return
    # Local file
    path = Path(log_uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

def close_logger(logger: logging.Logger):
    for h in list(logger.handlers):
        try:
            h.flush()
        except Exception:
            pass
        try:
            h.close()
        except Exception:
            pass
        logger.removeHandler(h)
