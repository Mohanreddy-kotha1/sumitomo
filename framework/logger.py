import logging, sys, json, os, datetime

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "ts": datetime.datetime.utcfromtimestamp(record.created).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "env": os.getenv("ENV", "local"),
        }
        if record.exc_info:
            log["exc"] = self.formatException(record.exc_info)
        return json.dumps(log)

def get_logger(name="app", level=None):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(level or os.getenv("LOG_LEVEL", "INFO"))
    logger.propagate = False
    return logger
