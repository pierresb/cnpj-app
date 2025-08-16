import streamlit as st
from lib.loaders import query

st.title("🏢 Empresas (Dados Cadastrais)")
st.caption("Inclui CNPJ Básico, Razão Social, Natureza Jurídica, Qualificação do Responsável, Capital Social, Porte, EFR. ")  #  [oai_citation:5‡cnpj-metadados.pdf](file-service://file-4FbedjZ88gZTDVnRZxrVtG)

f_porte = st.multiselect("Porte", ["00","01","03","05"])
f_natureza = st.text_input("Natureza Jurídica (código começa com...)")
f_razao = st.text_input("Razão Social (contém)")

sql = """
SELECT
  "CNPJ BÁSICO" as cnpj_basico,
  "RAZÃO SOCIAL / NOME EMPRESARIAL" as razao_social,
  "NATUREZA JURÍDICA" as natureza_juridica,
  "QUALIFICAÇÃO DO RESPONSÁVEL" as qualif_resp,
  "CAPITAL SOCIAL DA EMPRESA" as capital_social,
  "PORTE DA EMPRESA" as porte,
  "ENTE FEDERATIVO RESPONSÁVEL" as efr
FROM empresas WHERE 1=1
"""
params = []
if f_porte: sql += f" AND \"PORTE DA EMPRESA\" IN ({','.join(['?']*len(f_porte))})"; params += f_porte
if f_natureza: sql += " AND CAST(\"NATUREZA JURÍDICA\" AS TEXT) LIKE ?"; params.append(f"{f_natureza}%")
if f_razao: sql += " AND LOWER(\"RAZÃO SOCIAL / NOME EMPRESARIAL\") LIKE ?"; params.append(f"%{f_razao.lower()}%")
sql += " LIMIT 1000"

if st.button("Buscar"):
    df = query(sql, tuple(params))
    st.dataframe(df, use_container_width=True)