import streamlit as st
from lib.loaders import query

st.title("📚 Tabelas de Domínio (Países, Municípios, Qualificações, Naturezas, CNAEs)")
tab = st.selectbox("Domínio", ["paises", "municipios", "qualificacoes", "naturezas", "cnaes"])
sql = f"SELECT * FROM {tab} LIMIT 5000"
if st.button("Ver"):
    st.dataframe(query(sql), use_container_width=True)