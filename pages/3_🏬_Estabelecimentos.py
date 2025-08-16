# pages/3_🏬_Estabelecimentos.py
import streamlit as st
from lib.loaders import query, df_to_csv_bytes, df_to_parquet_bytes
from lib.ui import inject_global_css

st.set_page_config(page_title="🏬 Estabelecimentos", page_icon="🏬", layout="wide")
inject_global_css()

st.title("🏬 Estabelecimentos — Enriquecido (Município, País, Natureza)")

id_mf = st.selectbox("Matriz/Filial", ["", "1", "2"], help="1=matriz, 2=filial")
uf = st.text_input("UF", max_chars=2)
cnae = st.text_input("CNAE Principal (ex.: 6201501)")
nat_prefix = st.text_input("Natureza Jurídica (código começa com...)")

sql = """
WITH est AS (
  SELECT
    "CNPJ BÁSICO" as cnpj_basico,
    "CNPJ ORDEM" as ordem,
    "CNPJ DV" as dv,
    "IDENTIFICADOR MATRIZ/FILIAL" as id_mf,
    "NOME FANTASIA" as nome_fantasia,
    "UF" as uf,
    "MUNICÍPIO" as municipio,
    "PAIS" as pais_cod,
    "SITUAÇÃO CADASTRAL" as situacao,
    "DATA SITUAÇÃO CADASTRAL" as data_sit,
    "CNAE FISCAL PRINCIPAL" as cnae_principal,
    "CNAE FISCAL SECUNDÁRIA" as cnae_secund
  FROM estabelecimentos
),
emp AS (
  SELECT "CNPJ BÁSICO" as cnpj_basico, "RAZÃO SOCIAL / NOME EMPRESARIAL" as razao,
         "NATUREZA JURÍDICA" as natura
  FROM empresas
),
mun AS ( SELECT CAST(codigo AS TEXT) AS codigo, descricao AS municipio_nome FROM municipios ),
pais AS ( SELECT CAST(codigo AS TEXT) AS codigo, descricao AS pais_nome FROM paises ),
nat AS  ( SELECT CAST(codigo AS TEXT) AS codigo, descricao AS natureza_nome FROM naturezas )
SELECT
  CONCAT(LPAD(est.cnpj_basico,8,'0'), LPAD(est.ordem,4,'0'), LPAD(est.dv,2,'0')) AS cnpj14,
  emp.razao, emp.natura, nat.natureza_nome,
  est.nome_fantasia, est.uf, est.municipio, mun.municipio_nome,
  est.pais_cod, pais.pais_nome,
  est.situacao, est.data_sit,
  est.cnae_principal, est.cnae_secund
FROM est
LEFT JOIN emp  ON emp.cnpj_basico = est.cnpj_basico
LEFT JOIN mun  ON mun.codigo = CAST(est.municipio AS TEXT)
LEFT JOIN pais ON pais.codigo = CAST(est.pais_cod AS TEXT)
LEFT JOIN nat  ON nat.codigo = CAST(emp.natura AS TEXT)
WHERE 1=1
"""
params=[]
if id_mf: sql += " AND est.id_mf = ?"; params.append(id_mf)
if uf: sql += " AND est.uf = ?"; params.append(uf.upper())
if cnae: sql += " AND est.cnae_principal = ?"; params.append(cnae)
if nat_prefix: sql += " AND CAST(emp.natura AS TEXT) LIKE ?"; params.append(f"{nat_prefix}%")
sql += " LIMIT 5000"

if st.button("Buscar", type="primary"):
    df = query(sql, tuple(params))
    if df.empty:
        st.info("Sem resultados.")
    else:
        st.success(f"{len(df)} linha(s).")
        st.dataframe(df, use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            st.download_button("⬇️ Exportar CSV (;)", data=df_to_csv_bytes(df),
                               file_name="estabelecimentos_enriquecido.csv", mime="text/csv")
        with c2:
            st.download_button("⬇️ Exportar Parquet", data=df_to_parquet_bytes(df),
                               file_name="estabelecimentos_enriquecido.parquet", mime="application/octet-stream")