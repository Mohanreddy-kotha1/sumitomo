from pathlib import Path
import glob, os
import pandas as pd

def excel_to_csv_dir(src_dir: str, dst_dir: str):
    Path(dst_dir).mkdir(parents=True, exist_ok=True)
    xls = glob.glob(os.path.join(src_dir, "*.xlsx")) + glob.glob(os.path.join(src_dir, "*.xls"))
    outputs = []
    for x in xls:
        df = pd.read_excel(x)
        out = Path(dst_dir) / (Path(x).stem + ".csv")
        df.to_csv(out, index=False)
        outputs.append(str(out))
    return outputs
