# lib/loaders.py
# Pipeline robusto para lidar com os ZIPs/CSVs da RFB (mesmo quando o arquivo interno não tem extensão),
# converter para Parquet e consultar via DuckDB.

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from typing import List, Tuple

import duckdb
import pandas as pd
import requests
from tqdm import tqdm

# ------------------------------------------------------------------------------
# Paths e inicialização
# ------------------------------------------------------------------------------
DATA = Path("data")
DATA.mkdir(exist_ok=True)

DB_PATH = (DATA / "cnpj.duckdb").as_posix()


# ------------------------------------------------------------------------------
# Conexão / Execução de consultas
# ------------------------------------------------------------------------------
def open_con() -> duckdb.DuckDBPyConnection:
    """Abre conexão com o banco DuckDB local."""
    return duckdb.connect(DB_PATH, read_only=False)


def query(sql: str, params: Tuple | None = None) -> pd.DataFrame:
    """Executa uma consulta SQL no banco DuckDB e retorna DataFrame."""
    con = open_con()
    try:
        return con.execute(sql, params or ()).fetchdf()
    finally:
        con.close()


# ------------------------------------------------------------------------------
# Importação para tabelas DuckDB a partir de arquivos Parquet
# ------------------------------------------------------------------------------
def ensure_table_from_parquet(name: str, parquet_path: Path, replace: bool = False) -> None:
    """
    Garante que a tabela 'name' exista e esteja carregada a partir do Parquet informado.
    Se replace=True, recria a tabela do zero.
    """
    con = open_con()
    try:
        if replace:
            con.execute(f"DROP TABLE IF EXISTS {name}")
        # Cria a tabela vazia com o schema do parquet (caso ainda não exista)
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {name} AS "
            f"SELECT * FROM parquet_scan('{parquet_path.as_posix()}') LIMIT 0"
        )
        # Insere todos os registros do parquet
        con.execute(
            f"INSERT INTO {name} SELECT * FROM parquet_scan('{parquet_path.as_posix()}')"
        )
    finally:
        con.close()


# ------------------------------------------------------------------------------
# Download utilitário (com barra de progresso)
# ------------------------------------------------------------------------------
def download_zip(url: str, out_zip: Path) -> Path:
    """
    Baixa um ZIP (streaming) para 'out_zip'.
    """
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(out_zip, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=out_zip.name
        ) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
    return out_zip


# ------------------------------------------------------------------------------
# Escolha do arquivo correto dentro do ZIP (mesmo sem extensão)
# ------------------------------------------------------------------------------
def _choose_zip_member(
    zf: zipfile.ZipFile, prefer_keywords: List[str] | None = None
) -> zipfile.ZipInfo:
    """
    Escolhe o melhor membro do ZIP para leitura tabular.

    Regras:
      - ignora diretórios;
      - se prefer_keywords for informado, tenta casar por palavra-chave no nome (case-insensitive)
        e pega o MAIOR entre os que casarem;
      - caso contrário, pega o MAIOR arquivo do ZIP (normalmente é o CSV principal).
    """
    members = [m for m in zf.infolist() if not m.is_dir()]
    if not members:
        raise FileNotFoundError("Nenhum arquivo elegível dentro do ZIP.")

    if prefer_keywords:
        prefer_lower = [k.lower() for k in prefer_keywords]
        by_kw = [m for m in members if any(k in m.filename.lower() for k in prefer_lower)]
        if by_kw:
            return max(by_kw, key=lambda m: m.file_size)

    return max(members, key=lambda m: m.file_size)


def extract_tabular_from_zip(zip_path: Path, prefer_keywords: List[str] | None = None) -> io.BytesIO:
    """
    Extrai bytes do arquivo principal do ZIP, mesmo sem extensão.
    Faz uma checagem leve nas primeiras linhas para confirmar que é "CSV-like" (texto; possivelmente com ';').
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        member = _choose_zip_member(z, prefer_keywords)
        raw = z.read(member)

    # Checagem leve de "CSV-like"
    head = raw[:4096]
    try:
        sample = head.decode("latin1", errors="ignore")
    except Exception:
        sample = head.decode("utf-8", errors="ignore")

    # Se não tiver ';' na primeira linha, ainda pode ser CSV sem cabeçalho.
    # Mantemos tolerante; a validação real ocorrerá na leitura por pandas.
    # Você pode tornar estrito (raise) se preferir.
    _ = sample.splitlines()[0] if sample else ""

    return io.BytesIO(raw)


# ------------------------------------------------------------------------------
# Leitura de CSV gigante (separador ';') -> Parquet (com fallback de encoding)
# ------------------------------------------------------------------------------
def _read_csv_iterator(fobj: io.BytesIO | str, chunksize: int):
    """
    Tenta latin1 (padrão dos arquivos da RFB) e cai para utf-8 se necessário.
    Retorna um iterador de chunks do pandas.
    """
    try:
        return pd.read_csv(
            fobj,
            sep=";",
            dtype=str,
            chunksize=chunksize,
            encoding="latin1",
            low_memory=False,
        )
    except UnicodeDecodeError:
        if hasattr(fobj, "seek"):
            fobj.seek(0)
        return pd.read_csv(
            fobj,
            sep=";",
            dtype=str,
            chunksize=chunksize,
            encoding="utf-8",
            low_memory=False,
        )


def read_csv_semicolon_to_parquet(
    fobj: io.BytesIO | str, name: str, chunksize: int = 400_000
) -> Path:
    """
    Lê um CSV (separador ';') em chunks e materializa um único arquivo Parquet consolidado.
    Usa arquivos parquet temporários + DuckDB para concatenar rapidamente.
    """
    it = _read_csv_iterator(fobj, chunksize)

    parts: List[str] = []
    for i, chunk in enumerate(it):
        # normalização leve (opcional): garantir string em todas as colunas
        for c in chunk.columns:
            if chunk[c].dtype != "object":
                chunk[c] = chunk[c].astype("string")

        path = DATA / f"tmp_{name}_{i}.parquet"
        chunk.to_parquet(path, index=False)
        parts.append(path.as_posix())

    if not parts:
        raise ValueError("Nenhum chunk lido do CSV. Verifique o arquivo de origem.")

    con = open_con()
    try:
        final_path = DATA / f"{name}.parquet"
        # Concatena todos os temporários em um parquet único de saída
        con.execute(
            f"COPY (SELECT * FROM parquet_scan({parts})) "
            f"TO '{final_path.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    # Limpa temporários
    for p in parts:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception:
            pass

    return final_path


# ------------------------------------------------------------------------------
# Helpers de preparação integrados (download -> extrair -> parquet -> tabela)
# ------------------------------------------------------------------------------
def prepare_from_zip_url(url: str, name: str) -> Path:
    """
    Baixa um ZIP de 'url', extrai o arquivo tabular principal, converte para Parquet e carrega na tabela 'name'.
    Retorna o caminho do Parquet final.
    """
    zip_path = DATA / f"{name}.zip"
    download_zip(url, zip_path)
    fobj = extract_tabular_from_zip(zip_path, prefer_keywords=[name])
    parquet = read_csv_semicolon_to_parquet(fobj, name)
    ensure_table_from_parquet(name, parquet, replace=True)
    return parquet


def prepare_from_uploaded_zip_bytes(zip_bytes: bytes, name: str) -> Path:
    """
    Recebe os bytes de um ZIP enviado pelo usuário (upload),
    extrai o arquivo tabular principal, converte para Parquet e carrega na tabela 'name'.
    """
    tmp_zip = DATA / f"tmp_upload_{name}.zip"
    tmp_zip.write_bytes(zip_bytes)
    try:
        fobj = extract_tabular_from_zip(tmp_zip, prefer_keywords=[name])
        parquet = read_csv_semicolon_to_parquet(fobj, name)
        ensure_table_from_parquet(name, parquet, replace=True)
        return parquet
    finally:
        try:
            tmp_zip.unlink(missing_ok=True)
        except Exception:
            pass


def prepare_from_uploaded_csv_bytes(csv_bytes: bytes, name: str) -> Path:
    """
    Recebe os bytes de um CSV enviado pelo usuário (upload),
    converte para Parquet e carrega na tabela 'name'.
    """
    fobj = io.BytesIO(csv_bytes)
    parquet = read_csv_semicolon_to_parquet(fobj, name)
    ensure_table_from_parquet(name, parquet, replace=True)
    return parquet


# ------------------------------------------------------------------------------
# (Opcional) utilitário simples para salvar DataFrame em parquet
# ------------------------------------------------------------------------------
def save_parquet(df: pd.DataFrame, name: str) -> Path:
    """
    Salva um DataFrame diretamente em Parquet dentro de /data com o nome fornecido.
    """
    path = DATA / f"{name}.parquet"
    df.to_parquet(path, index=False)
    return path

# ======== ACRESCENTAR AO FINAL DO ARQUIVO loaders.py =========
from datetime import datetime

# ---- Catálogo (tabela de controle) ---------------------------------
def _ensure_catalog():
    con = open_con()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog (
              id IDENTITY,
              dataset TEXT NOT NULL,          -- ex.: 'empresas', 'estabelecimentos'...
              month_ref TEXT NOT NULL,        -- '2025-06'
              source_url TEXT,
              parquet_path TEXT NOT NULL,
              rows BIGINT,
              loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    finally:
        con.close()

def _count_rows_in_parquet(parquet_path: Path) -> int:
    con = open_con()
    try:
        return con.execute(
            f"SELECT COUNT(*) AS n FROM parquet_scan('{parquet_path.as_posix()}')"
        ).fetchone()[0]
    finally:
        con.close()

def add_to_catalog(dataset: str, month_ref: str, parquet_path: Path, source_url: str | None, rows: int | None = None):
    _ensure_catalog()
    if rows is None:
        try:
            rows = _count_rows_in_parquet(parquet_path)
        except Exception:
            rows = None
    con = open_con()
    try:
        con.execute(
            "INSERT INTO catalog (dataset, month_ref, source_url, parquet_path, rows, loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
            (dataset, month_ref, source_url or "", parquet_path.as_posix(), rows, datetime.now())
        )
    finally:
        con.close()

def get_catalog():
    _ensure_catalog()
    return query("SELECT id, dataset, month_ref, source_url, parquet_path, rows, loaded_at FROM catalog ORDER BY loaded_at DESC")

# ---- Bulk download por mês/ano -------------------------------------
_EXPECTED_FILES = {
    # Você pode ajustar ou expandir conforme a pasta do mês no portal
    # Chaves são nomes “lógicos” para escolher keywords corretas
    "empresas": ["Empresas1.zip", "Empresas2.zip"],
    "estabelecimentos": ["Estabelecimentos1.zip", "Estabelecimentos2.zip", "Estabelecimentos3.zip"],
    "socios": ["Socios1.zip", "Socios2.zip"],
    "simples": ["Simples.zip"],
    "paises": ["Paises.zip"],
    "municipios": ["Municipios.zip"],
    "qualificacoes": ["Qualificacoes.zip"],
    "naturezas": ["Naturezas.zip"],
    "cnaes": ["Cnaes.zip"],
}

# Palavras-chave para achar o arquivo “sem extensão” dentro do zip
_DATASET_KEYWORDS = {
    "empresas": ["empresas", "empresa"],
    "estabelecimentos": ["estabelec", "estabelecimentos"],
    "socios": ["socios", "sócios", "socio"],
    "simples": ["simples", "mei"],
    "paises": ["paises", "países", "pais"],
    "municipios": ["municipio", "municípios", "municipios"],
    "qualificacoes": ["qualificacao", "qualificações", "qualificacoes"],
    "naturezas": ["natureza", "naturezas"],
    "cnaes": ["cnae", "cnaes"],
}

def month_dir_url(year: int, month: int) -> str:
    # Ex.: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-06/
    return f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year:04d}-{month:02d}/"

def prepare_all_for_month(year: int, month: int, datasets: list[str] | None = None) -> list[tuple[str, Path]]:
    """
    Faz download e prepara TODOS os conjuntos desejados para o mês/ano.
    - datasets=None => prepara todos os chaves de _EXPECTED_FILES
    Retorna lista [(dataset, parquet_path), ...]
    """
    base = month_dir_url(year, month)
    targets = datasets or list(_EXPECTED_FILES.keys())
    prepared = []

    for dataset in targets:
        files = _EXPECTED_FILES.get(dataset, [])
        keywords = _DATASET_KEYWORDS.get(dataset, [dataset])
        for fname in files:
            url = base + fname
            try:
                zip_path = DATA / f"{dataset}_{fname}"
                download_zip(url, zip_path)
                fobj = extract_tabular_from_zip(zip_path, prefer_keywords=keywords)
                parquet = read_csv_semicolon_to_parquet(fobj, dataset)
                ensure_table_from_parquet(dataset, parquet, replace=True)
                add_to_catalog(dataset, f"{year:04d}-{month:02d}", parquet, url)
                prepared.append((dataset, parquet))
            except Exception as e:
                # segue para o próximo arquivo; é comum alguns meses não terem todos os pacotes
                print(f"[WARN] Falhou em {url}: {e}")
    return prepared

# ---- Parquet/CSV em memória (para exportar) -------------------------
def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";").encode("utf-8")

def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    df.to_parquet(bio, index=False)
    return bio.getvalue()
# ======== FIM DO BLOCO NOVO loaders.py =========
