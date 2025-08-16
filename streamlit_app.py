import streamlit as st
from pathlib import Path
from lib.loaders import download_zip, extract_first_csv, read_csv_semicolon_to_parquet, ensure_table_from_parquet
from lib.loaders import query

st.set_page_config(page_title="CNPJ Open Data Explorer", page_icon="🗂️", layout="wide")

st.title("🗂️ CNPJ — Consulta de Dados (RFB Dados Abertos)")
st.caption("Leituras seguem o dicionário oficial (campos, separador ';', regras específicas).")  #  [oai_citation:4‡cnpj-metadados.pdf](file-service://file-4FbedjZ88gZTDVnRZxrVtG)

with st.expander("⚙️ Carregar dados"):
    st.markdown("Opções: fornecer **URL do ZIP** (RFB), **fazer upload** do ZIP/CSV, ou pular se já tiver .parquet em `data/`.")
    fonte = st.radio("Fonte", ["URL ZIP (RFB)", "Upload ZIP/CSV", "Já possuo .parquet"], horizontal=True)
    tabela = st.selectbox("Tabela", ["empresas", "estabelecimentos", "socios", "simples",
                                     "paises", "municipios", "qualificacoes", "naturezas", "cnaes"])
    name = tabela

    if fonte == "URL ZIP (RFB)":
        url = st.text_input("URL do ZIP (direto do portal RFB)", placeholder="https://.../Empresas1.zip")
        if st.button("Baixar e preparar", type="primary", use_container_width=True, disabled=not url):
            zip_path = Path("data") / f"{name}.zip"
            try:
                download_zip(url, zip_path)
                fobj = extract_first_csv(zip_path)
                parquet = read_csv_semicolon_to_parquet(fobj, name)
                ensure_table_from_parquet(name, parquet, replace=True)
                st.success(f"{name} pronto em {parquet}")
            except Exception as e:
                st.error(f"Falha ao preparar: {e}")

    elif fonte == "Upload ZIP/CSV":
        up = st.file_uploader("Selecione ZIP (com CSV) ou CSV", type=["zip","csv"])
        if up and st.button("Preparar", type="primary", use_container_width=True):
            try:
                if up.name.lower().endswith(".zip"):
                    import io, zipfile
                    data = io.BytesIO(up.read())
                    fobj = extract_first_csv(Path("data/tmp.zip").write_bytes(data.getvalue()) or Path("data/tmp.zip"))
                    parquet = read_csv_semicolon_to_parquet(fobj, name)
                else:
                    parquet = read_csv_semicolon_to_parquet(io.BytesIO(up.read()), name)
                ensure_table_from_parquet(name, parquet, replace=True)
                st.success(f"{name} pronto em {parquet}")
            except Exception as e:
                st.error(f"Falha ao preparar: {e}")
    else:
        st.info("Nenhuma ação: as consultas usarão os .parquet já existentes em /data e a base DuckDB.")