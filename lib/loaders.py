import os, io, zipfile, duckdb, pandas as pd, requests
from pathlib import Path
from tqdm import tqdm

DATA = Path("data"); DATA.mkdir(exist_ok=True)
DB = (DATA / "cnpj.duckdb").as_posix()

def _save_parquet(df: pd.DataFrame, name: str):
    path = DATA / f"{name}.parquet"
    df.to_parquet(path, index=False)
    return path

def open_con():
    return duckdb.connect(DB, read_only=False)

def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    con = open_con()
    try:
        return con.execute(sql, params or ()).fetchdf()
    finally:
        con.close()

def ensure_table_from_parquet(name: str, parquet_path: Path, replace: bool=False):
    con = open_con()
    try:
        if replace:
            con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM parquet_scan('{parquet_path.as_posix()}') LIMIT 0")
        con.execute(f"INSERT INTO {name} SELECT * FROM parquet_scan('{parquet_path.as_posix()}')")
    finally:
        con.close()

def read_csv_semicolon_to_parquet(fobj: io.BytesIO | str, name: str, chunksize: int=400_000):
    # leitura em chunks para CSVs enormes com separador ';' (conforme layout).  [oai_citation:3‡cnpj-metadados.pdf](file-service://file-4FbedjZ88gZTDVnRZxrVtG)
    it = pd.read_csv(fobj, sep=";", dtype=str, chunksize=chunksize, encoding="latin1", low_memory=False)
    first = True; parts = []
    for i, chunk in enumerate(it):
        path = DATA / f"tmp_{name}_{i}.parquet"
        chunk.to_parquet(path, index=False)
        parts.append(path.as_posix())
        first = False
    # concatena em um parquet único via duckdb (rápido)
    con = open_con()
    try:
        final_path = DATA / f"{name}.parquet"
        con.execute(f"COPY (SELECT * FROM parquet_scan({parts})) TO '{final_path.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.close()
    # cleanup
    for p in parts: Path(p).unlink(missing_ok=True)
    return final_path

def download_zip(url: str, out_zip: Path) -> Path:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(out_zip, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=out_zip.name) as pbar:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk); pbar.update(len(chunk))
    return out_zip

def extract_first_csv(zip_path: Path) -> io.BytesIO:
    with zipfile.ZipFile(zip_path, "r") as z:
        # pega o primeiro CSV do zip
        for info in z.infolist():
            if info.filename.lower().endswith(".csv"):
                return io.BytesIO(z.read(info))
    raise FileNotFoundError("CSV não encontrado no ZIP")