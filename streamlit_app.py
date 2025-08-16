# app.py
# App base (home) para preparação dos dados CNPJ:
# - Baixar ZIP direto da RFB e preparar a tabela
# - Upload de ZIP (ou CSV) e preparar a tabela
# - Visualizar amostra (LIMIT 50) da tabela carregada

import io
from pathlib import Path

import streamlit as st

from lib.loaders import (
    prepare_from_zip_url,
    prepare_from_uploaded_zip_bytes,
    prepare_from_uploaded_csv_bytes,
    query,
)

st.set_page_config(
    page_title="CNPJ — Preparação de Dados (RFB Dados Abertos)",
    page_icon="🗂️",
    layout="wide",
)

st.title("🗂️ CNPJ — Preparação e Consulta Básica")
st.caption(
    "Carregue os conjuntos **Empresas, Estabelecimentos, Sócios, Simples e Domínios** "
    "a partir dos pacotes da RFB (ZIP) ou de arquivos locais. Em seguida, visualize uma amostra."
)

# ---------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------
st.sidebar.header("Carregar dados")
fonte = st.sidebar.radio(
    "Fonte de dados",
    ["URL ZIP (RFB)", "Upload ZIP/CSV", "Já possuo .parquet"],
)

tabela = st.sidebar.selectbox(
    "Tabela alvo",
    [
        "empresas",
        "estabelecimentos",
        "socios",
        "simples",
        "paises",
        "municipios",
        "qualificacoes",
        "naturezas",
        "cnaes",
    ],
    help=(
        "Escolha qual tabela será preparada/carregada. "
        "Os nomes acima correspondem ao nome da tabela no DuckDB."
    ),
)

st.sidebar.divider()
st.sidebar.caption(
    "Dica: os pacotes da RFB às vezes trazem o arquivo interno **sem extensão**. "
    "Este app lida com isso automaticamente."
)

# ---------------------------------------------------------------------
# Área principal — controles conforme fonte escolhida
# ---------------------------------------------------------------------
with st.expander("⚙️ Preparar/Carregar dados", expanded=True):
    if fonte == "URL ZIP (RFB)":
        url = st.text_input(
            "URL do ZIP da RFB",
            placeholder="Ex.: https://arquivos.receitafederal.gov.br/.../Empresas1.zip",
        )
        col_a, col_b = st.columns([1, 2])
        if col_a.button(
            "Baixar e preparar",
            type="primary",
            use_container_width=True,
            disabled=not url,
        ):
            try:
                parquet_path = prepare_from_zip_url(url, tabela)
                st.success(f"Tabela **{tabela}** preparada a partir de: `{parquet_path}`")
            except Exception as e:
                st.error(f"Falha ao preparar: {e}")

    elif fonte == "Upload ZIP/CSV":
        up = st.file_uploader(
            "Selecione um arquivo ZIP (com o dado principal dentro) ou um CSV",
            type=["zip", "csv"],
        )
        if st.button(
            "Preparar do upload",
            type="primary",
            use_container_width=True,
            disabled=up is None,
        ):
            try:
                if up is None:
                    st.warning("Envie um arquivo para continuar.")
                else:
                    if up.name.lower().endswith(".zip"):
                        parquet_path = prepare_from_uploaded_zip_bytes(up.read(), tabela)
                    else:
                        parquet_path = prepare_from_uploaded_csv_bytes(up.read(), tabela)
                    st.success(
                        f"Tabela **{tabela}** preparada a partir do upload: `{parquet_path}`"
                    )
            except Exception as e:
                st.error(f"Falha ao preparar: {e}")

    else:  # Já possuo .parquet
        st.info(
            "Se você já tem os arquivos `.parquet` em `/data`, as páginas de consulta "
            "irão usá-los. Nada a fazer aqui."
        )

st.divider()

# ---------------------------------------------------------------------
# Pré-visualização (LIMIT 50)
# ---------------------------------------------------------------------
st.subheader("🔎 Pré-visualização (LIMIT 50)")
st.caption(
    "Mostra 50 linhas da tabela selecionada no banco DuckDB local, se existir."
)

query_btn = st.button("Exibir amostra da tabela", use_container_width=True)
if query_btn:
    try:
        df = query(f"SELECT * FROM {tabela} LIMIT 50")
        if df.empty:
            st.warning("A tabela existe, mas não retornou linhas (ou está vazia).")
        else:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(
            "Não foi possível consultar a tabela. "
            "Verifique se ela já foi preparada/carregada."
        )
        st.exception(e)

# ---------------------------------------------------------------------
# Rodapé
# ---------------------------------------------------------------------
st.divider()
st.caption(
    "Observações: leitura em chunks com separador `;`, tolerante a arquivo interno sem extensão; "
    "conversão para Parquet e carga no DuckDB para consultas rápidas."
)