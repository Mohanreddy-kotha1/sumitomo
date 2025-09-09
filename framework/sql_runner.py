from pathlib import Path

def render_sql(template_path: str, **kwargs) -> str:
    sql = Path(template_path).read_text(encoding="utf-8")
    # simple Python format-style templating
    return sql.format(**kwargs)

def run_sql(spark, sql_text: str):
    return spark.sql(sql_text)
