import streamlit as st
from lib.loaders import query

st.title("💡 Simples/MEI")
op_simples = st.selectbox("Opção Simples", ["", "S","N"])
op_mei = st.selectbox("Opção MEI", ["", "S","N"])

sql = """
SELECT
  "CNPJ BÁSICO" as cnpj_basico,
  "OPÇÃO PELO SIMPLES" as opcao_simples,
  "DATA DE OPÇÃO PELO SIMPLES" as dt_op_simples,
  "DATA DE EXCLUSÃO DO SIMPLES" as dt_exc_simples,
  "OPÇÃO PELO MEI" as opcao_mei,
  "DATA DE OPÇÃO PELO MEI" as dt_op_mei,
  "DATA DE EXCLUSÃO DO MEI" as dt_exc_mei
FROM simples WHERE 1=1
"""
params = []
if op_simples: sql += " AND \"OPÇÃO PELO SIMPLES\" = ?"; params.append(op_simples)
if op_mei: sql += " AND \"OPÇÃO PELO MEI\" = ?"; params.append(op_mei)
sql += " LIMIT 1000"

if st.button("Buscar"):
    st.dataframe(query(sql, tuple(params)), use_container_width=True)