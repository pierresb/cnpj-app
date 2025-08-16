import streamlit as st
from lib.loaders import query
from lib.util import compose_cnpj

st.title("🏬 Estabelecimentos")
id_mf = st.selectbox("Matriz/Filial", ["", "1", "2"], help="1=matriz, 2=filial")  #  [oai_citation:6‡cnpj-metadados.pdf](file-service://file-4FbedjZ88gZTDVnRZxrVtG)
uf = st.text_input("UF", max_chars=2)
cnae = st.text_input("CNAE Principal (ex.: 6201501)")

sql = """
SELECT
  "CNPJ BÁSICO" as cnpj_basico,
  "CNPJ ORDEM" as ordem,
  "CNPJ DV" as dv,
  "IDENTIFICADOR MATRIZ/FILIAL" as id_mf,
  "NOME FANTASIA" as nome_fantasia,
  "UF" as uf,
  "MUNICÍPIO" as municipio,
  "SITUAÇÃO CADASTRAL" as situacao,
  "DATA SITUAÇÃO CADASTRAL" as data_sit,
  "CNAE FISCAL PRINCIPAL" as cnae_principal,
  "CNAE FISCAL SECUNDÁRIA" as cnae_secund
FROM estabelecimentos WHERE 1=1
"""
params = []
if id_mf: sql += " AND \"IDENTIFICADOR MATRIZ/FILIAL\" = ?"; params.append(id_mf)
if uf: sql += " AND \"UF\" = ?"; params.append(uf.upper())
if cnae: sql += " AND \"CNAE FISCAL PRINCIPAL\" = ?"; params.append(cnae)
sql += " LIMIT 1000"

if st.button("Buscar"):
    df = query(sql, tuple(params))
    if not df.empty:
        df["CNPJ"] = [compose_cnpj(a,b,c) for a,b,c in zip(df["cnpj_basico"], df["ordem"], df["dv"])]
        st.dataframe(df[["CNPJ","nome_fantasia","uf","municipio","situacao","data_sit","cnae_principal","cnae_secund"]],
                     use_container_width=True)
    else:
        st.info("Sem resultados.")