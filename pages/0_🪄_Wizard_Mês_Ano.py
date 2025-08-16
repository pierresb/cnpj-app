# pages/0_🪄_Wizard_Mês_Ano.py
import streamlit as st
from lib.loaders import prepare_all_for_month, get_catalog
from lib.ui import inject_global_css

st.set_page_config(page_title="🪄 Wizard — Baixar por Mês/Ano", page_icon="🪄", layout="wide")
inject_global_css()

st.title("🪄 Wizard — Baixar todos os pacotes por Mês/Ano")
colA, colB = st.columns(2)
year = colA.number_input("Ano", min_value=2018, max_value=2100, value=2025, step=1)
month = colB.number_input("Mês", min_value=1, max_value=12, value=6, step=1)

targets = st.multiselect(
    "Conjuntos a baixar (opcional):",
    ["empresas","estabelecimentos","socios","simples","paises","municipios","qualificacoes","naturezas","cnaes"],
    default=["empresas","estabelecimentos","socios","simples"]
)
if st.button("▶️ Baixar e preparar", type="primary"):
    with st.spinner("Baixando e preparando..."):
        prepared = prepare_all_for_month(int(year), int(month), targets or None)
    if prepared:
        st.success(f"Finalizado. Conjuntos preparados: {', '.join(sorted(set([d for d,_ in prepared])))}")
    else:
        st.warning("Nenhum pacote foi preparado. Verifique o mês/ano e os conjuntos.")

st.subheader("📒 Catálogo de cargas")
cat = get_catalog()
st.dataframe(cat, use_container_width=True)