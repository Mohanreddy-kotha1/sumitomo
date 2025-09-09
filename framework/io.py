from pathlib import Path
import re, csv
import pandas as pd
from .logger import get_logger

try:
    import chardet
except Exception:
    chardet = None  # optional

def _slug(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip())
    s = re.sub(r"[^A-Za-z0-9_.-]+", "", s)
    return s or "sheet"

def _read_excel_like(fp: Path, first_sheet_only: bool):
    """
    Try engines suitable for the extension, but fail over if necessary.
    Returns: list[(sheet_name, DataFrame)]
    """
    ext = fp.suffix.lower()
    engines = []
    if ext in {".xlsx", ".xlsm"}:
        engines = ["openpyxl"]
    elif ext in {".xls", ".xlx"}:       # <-- .xlx treated as legacy .xls
        engines = ["xlrd", "openpyxl"]   # try xlrd first, then openpyxl
    elif ext == ".xlsb":
        engines = ["pyxlsb"]
    else:
        engines = ["openpyxl", "xlrd", "pyxlsb"]  # last-ditch attempts

    last_err = None
    for eng in engines:
        try:
            xl = pd.ExcelFile(fp, engine=eng)
            sheets = xl.sheet_names[:1] if first_sheet_only else xl.sheet_names
            out = []
            for sh in sheets:
                df = pd.read_excel(xl, sheet_name=sh, dtype=str)
                out.append((sh, df))
            return out
        except Exception as e:
            last_err = e
    # If all engines failed, return None to allow CSV fallback
    raise RuntimeError(f"Excel parse failed for {fp.name}: {last_err}")

def _read_as_csv_with_sniff(fp: Path):
    # Try to detect encoding & delimiter for files that are actually text/CSV
    encodings = []
    if chardet:
        try:
            raw = fp.read_bytes()[:4096]
            enc_guess = chardet.detect(raw).get("encoding") or "utf-8"
            encodings = [enc_guess, "utf-8", "latin-1"]
        except Exception:
            encodings = ["utf-8", "latin-1"]
    else:
        encodings = ["utf-8", "latin-1"]

    delims = [",", ";", "\t", "|"]
    for enc in encodings:
        try:
            sample = fp.read_text(encoding=enc, errors="ignore")[:4096]
            try:
                sniffer = csv.Sniffer().sniff(sample, delimiters=";,|\t")
                delim = sniffer.delimiter
            except Exception:
                # fall back to a short list
                for d in delims:
                    try:
                        df = pd.read_csv(fp, encoding=enc, sep=d, dtype=str)
                        return df
                    except Exception:
                        continue
                continue
            df = pd.read_csv(fp, encoding=enc, sep=delim, dtype=str)
            return df
        except Exception:
            continue
    raise RuntimeError(f"Unable to parse {fp} as CSV/text with common encodings.")

def excel_to_csv_dir(src_dir, dst_dir, first_sheet_only=False, encoding="utf-8-sig"):
    """
    Recursively export spreadsheet-like files under src_dir to CSV under dst_dir,
    preserving subfolders. Supports: .xlsx, .xlsm, .xls, .xlx, .xlsb
    If a file is not a true Excel but text disguised (e.g., .xlx), it will be
    parsed as CSV using delimiter/encoding sniffing.
    Returns list of written CSV paths.
    """
    log = get_logger("excel2csv")
    src_dir = Path(src_dir)
    dst_dir = Path(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)

    patterns = ("*.xlsx", "*.xlsm", "*.xls", "*.xlx", "*.xlsb")
    excel_files = []
    for pat in patterns:
        excel_files.extend(src_dir.rglob(pat))

    if not excel_files:
        log.warning(f"No spreadsheet files found under {src_dir}")
        return []

    written = []
    for f in excel_files:
        rel = f.parent.relative_to(src_dir)  # preserve country/subfolder structure
        out_dir = (dst_dir / rel)
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            # First, try as real Excel (with appropriate engine failover)
            try:
                sheets = _read_excel_like(f, first_sheet_only)
                for sh, df in sheets:
                    out_name = f"{f.stem}.csv" if first_sheet_only else f"{f.stem}__{_slug(sh)}.csv"
                    out_path = out_dir / out_name
                    df.to_csv(out_path, index=False, encoding=encoding)
                    written.append(str(out_path))
                    log.info(f"Wrote CSV (excel): {out_path}")
            except Exception as excel_err:
                # Fallback: attempt to parse as CSV/text (handles misnamed .xlx)
                df = _read_as_csv_with_sniff(f)
                out_path = out_dir / f"{f.stem}.csv"
                df.to_csv(out_path, index=False, encoding=encoding)
                written.append(str(out_path))
                log.info(f"Wrote CSV (text-fallback): {out_path} (source: {f.name})")
        except Exception as e:
            log.error(f"FAILED {f} :: {e}")
    return written
