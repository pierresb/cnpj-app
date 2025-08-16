import streamlit as st
from lib.loaders import query

st.title("👥 Sócios (dados com anonimização de CPF/CNPJ conforme layout)")  #  [oai_citation:7‡cnpj-metadados.pdf](file-service://file-4FbedjZ88gZTDVnRZxrVtG)
nome = st.text_input("Nome/Razão do Sócio (contém)")
ident = st.selectbox("Identificador do Sócio", ["", "1", "2", "3"], help="1=Pessoa Jurídica, 2=Pessoa Física, 3=Estrangeiro")  #  [oai_citation:8‡cnpj-metadados.pdf](file-service://file-4FbedjZ88gZTDVnRZxrVtG)

sql = """
SELECT
  "CNPJ BÁSICO" as cnpj_basico,
  "IDENTIFICADOR DE SÓCIO" as ident_socio,
  "NOME DO SÓCIO (NO CASO PF) OU RAZÃO SOCIAL (NO CASO PJ)" as nome_razao,
  "CNPJ/CPF DO SÓCIO" as doc,
  "QUALIFICAÇÃO DO SÓCIO" as qualif,
  "DATA DE ENTRADA SOCIEDADE" as data_entrada_soc,
  "FAIXA ETÁRIA" as faixa_etaria
FROM socios WHERE 1=1
"""
params = []
if nome: sql += " AND LOWER(\"NOME DO SÓCIO (NO CASO PF) OU RAZÃO SOCIAL (NO CASO PJ)\") LIKE ?"; params.append(f"%{nome.lower()}%")
if ident: sql += " AND \"IDENTIFICADOR DE SÓCIO\" = ?"; params.append(ident)
sql += " LIMIT 1000"

if st.button("Buscar"):
    df = query(sql, tuple(params))
    st.dataframe(df, use_container_width=True)