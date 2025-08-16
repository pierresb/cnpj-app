import re

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def mask_cnpj(cnpj: str) -> str:
    d = only_digits(cnpj).zfill(14)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

def compose_cnpj(cnpj_basico, ordem, dv) -> str:
    d = f"{str(cnpj_basico).zfill(8)}{str(ordem).zfill(4)}{str(dv).zfill(2)}"
    return mask_cnpj(d)

def split_cnae_secundaria(s: str):
    if not s: return []
    # no layout, múltiplas ocorrências separadas por vírgula.  [oai_citation:2‡cnpj-metadados.pdf](file-service://file-4FbedjZ88gZTDVnRZxrVtG)
    return [x.strip() for x in str(s).split(",") if x.strip()]