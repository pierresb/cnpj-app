# app.py
# CNPJ — Preparação e Consulta Básica (com seleção do TIPO e botões de FONTE)
# Observação importante:
# - Para aceitar uploads de até 500 MB, configure .streamlit/config.toml com:
#   [server]
#   maxUploadSize = 500

from pathlib import Path
import io
import streamlit as st

from lib.loaders import (
    download_zip,
    extract_tabular_from_zip,
    read_csv_semicolon_to_parquet,
    ensure_table_from_parquet,
    query,
)

st.set_page_config(
    page_title="CNPJ — Preparação de Dados (RFB Dados Abertos)",
    page_icon="🗂️",
    layout="wide",
)

UPLOAD_LIMIT_MB = 500  # usado apenas para exibição e checagem no app

st.title("🗂️ CNPJ — Preparação e Consulta Básica")
st.caption(
    "Carregue os conjuntos da RFB. O app lida com arquivos internos **sem extensão** e usa Parquet + DuckDB."
)

# ---------------------------------------------------------------------
# Catálogo de TIPOS (define a tabela e palavras-chave para achar o arquivo no ZIP)
# ---------------------------------------------------------------------
DATASETS = {
    "empresas": {
        "table": "empresas",
        "keywords": ["empresas", "empresa", "empresas1", "empresa1"],
        "hint": "Cadastro de empresas (CNPJ Básico, razão social, capital, porte, natureza, etc.)",
    },
    "estabelecimentos": {
        "table": "estabelecimentos",
        "keywords": ["estabelec", "estabelecimentos"],
        "hint": "Estabelecimentos (CNPJ completo, nome fantasia, CNAE, endereço, UF/município, etc.)",
    },
    "socios": {
        "table": "socios",
        "keywords": ["socios", "sócios", "socio", "socio1"],
        "hint": "Sócios (identificador PF/PJ/estrangeiro, qualificação, datas, etc.)",
    },
    "simples": {
        "table": "simples",
        "keywords": ["simples", "mei"],
        "hint": "Opção pelo Simples/MEI (datas de opção/exclusão).",
    },
    "paises": {
        "table": "paises",
        "keywords": ["paises", "países", "pais"],
        "hint": "Tabela de domínio — Países.",
    },
    "municipios": {
        "table": "municipios",
        "keywords": ["municipio", "municípios", "municipios"],
        "hint": "Tabela de domínio — Municípios.",
    },
    "qualificacoes": {
        "table": "qualificacoes",
        "keywords": ["qualificacao", "qualificações", "qualificacoes"],
        "hint": "Tabela de domínio — Qualificações.",
    },
    "naturezas": {
        "table": "naturezas",
        "keywords": ["natureza", "naturezas"],
        "hint": "Tabela de domínio — Naturezas Jurídicas.",
    },
    "cnaes": {
        "table": "cnaes",
        "keywords": ["cnae", "cnaes"],
        "hint": "Tabela de domínio — CNAEs.",
    },
}

# ---------------------------------------------------------------------
# Estado e UI — seleção do TIPO e FONTE (com BOTÕES)
# ---------------------------------------------------------------------
if "fonte" not in st.session_state:
    st.session_state.fonte = "URL ZIP (RFB)"

col_tipo, col_hint = st.columns([1.2, 1.8], vertical_alignment="center")
with col_tipo:
    tipo = st.selectbox(
        "Tipo de arquivo (dataset)",
        list(DATASETS.keys()),
        index=1,  # padrão: estabelecimentos
        format_func=lambda k: k.capitalize(),
        help="Escolha o conjunto que você vai carregar/preparar agora.",
    )
with col_hint:
    st.info(DATASETS[tipo]["hint"])

st.markdown("**Selecione a fonte do arquivo:**")
b1, b2, b3 = st.columns(3)
with b1:
    if st.button("🌐 URL ZIP (RFB)", use_container_width=True):
        st.session_state.fonte = "URL ZIP (RFB)"
with b2:
    if st.button("🗜️ Upload ZIP", use_container_width=True):
        st.session_state.fonte = "Upload ZIP"
with b3:
    if st.button("🧾 Upload CSV", use_container_width=True):
        st.session_state.fonte = "Upload CSV"

st.caption(f"Fonte atual selecionada: **{st.session_state.fonte}**  •  Limite de upload: **{UPLOAD_LIMIT_MB} MB**")

# ---------------------------------------------------------------------
# Ações por FONTE
# ---------------------------------------------------------------------
table_name = DATASETS[tipo]["table"]
keywords = DATASETS[tipo]["keywords"]

st.divider()
with st.expander("⚙️ Preparar/Carregar dados", expanded=True):
    fonte = st.session_state.fonte

    if fonte == "URL ZIP (RFB)":
        url = st.text_input(
            "URL do ZIP da RFB",
            placeholder="Ex.: https://arquivos.receitafederal.gov.br/.../Empresas1.zip",
        )

        # (Opcional) Debug: listar conteúdo do ZIP remoto
        with st.popover("🔎 Ver arquivos dentro do ZIP (debug)"):
            if url:
                try:
                    import requests, zipfile
                    r = requests.get(url, timeout=60)
                    r.raise_for_status()
                    bio = io.BytesIO(r.content)
                    with zipfile.ZipFile(bio, "r") as z:
                        st.write([m.filename for m in z.infolist()])
                except Exception as e:
                    st.warning(f"Não foi possível listar: {e}")

        if st.button("Baixar e preparar", type="primary", use_container_width=True, disabled=not url):
            try:
                zip_path = Path("data") / f"{table_name}.zip"
                download_zip(url, zip_path)
                fobj = extract_tabular_from_zip(zip_path, prefer_keywords=keywords)
                parquet = read_csv_semicolon_to_parquet(fobj, table_name)
                ensure_table_from_parquet(table_name, parquet, replace=True)
                st.success(f"Tabela **{table_name}** preparada a partir de: `{parquet}`")
            except Exception as e:
                st.error(f"Falha ao preparar: {e}")

    elif fonte == "Upload ZIP":
        st.caption(f"Aceita até **{UPLOAD_LIMIT_MB} MB** (ajustado no config.toml).")
        up_zip = st.file_uploader("Selecione um arquivo ZIP", type=["zip"])
        if up_zip is not None:
            size_mb = getattr(up_zip, "size", 0) / (1024 * 1024)
            if size_mb > UPLOAD_LIMIT_MB:
                st.error(f"O arquivo tem {size_mb:.1f} MB e excede o limite de {UPLOAD_LIMIT_MB} MB.")
            else:
                if st.button("Preparar do ZIP", type="primary", use_container_width=True):
                    try:
                        tmp_zip = Path("data") / f"tmp_upload_{table_name}.zip"
                        tmp_zip.write_bytes(up_zip.read())
                        try:
                            fobj = extract_tabular_from_zip(tmp_zip, prefer_keywords=keywords)
                            parquet = read_csv_semicolon_to_parquet(fobj, table_name)
                            ensure_table_from_parquet(table_name, parquet, replace=True)
                            st.success(f"Tabela **{table_name}** preparada (upload ZIP): `{parquet}`")
                        finally:
                            tmp_zip.unlink(missing_ok=True)
                    except Exception as e:
                        st.error(f"Falha ao preparar: {e}")
        else:
            st.info("Envie um arquivo ZIP para prosseguir.")

    elif fonte == "Upload CSV":
        st.caption(f"Aceita até **{UPLOAD_LIMIT_MB} MB** (ajustado no config.toml).")
        up_csv = st.file_uploader("Selecione um arquivo CSV", type=["csv"])
        if up_csv is not None:
            size_mb = getattr(up_csv, "size", 0) / (1024 * 1024)
            if size_mb > UPLOAD_LIMIT_MB:
                st.error(f"O arquivo tem {size_mb:.1f} MB e excede o limite de {UPLOAD_LIMIT_MB} MB.")
            else:
                if st.button("Preparar do CSV", type="primary", use_container_width=True):
                    try:
                        parquet = read_csv_semicolon_to_parquet(io.BytesIO(up_csv.read()), table_name)
                        ensure_table_from_parquet(table_name, parquet, replace=True)
                        st.success(f"Tabela **{table_name}** preparada (upload CSV): `{parquet}`")
                    except Exception as e:
                        st.error(f"Falha ao preparar: {e}")
        else:
            st.info("Envie um arquivo CSV para prosseguir.")

    else:
        st.info(
            "Se você já tem os arquivos `.parquet` em `/data`, nada a fazer aqui. "
            "As consultas usarão essas tabelas no DuckDB."
        )

# ---------------------------------------------------------------------
# Pré-visualização (LIMIT 50)
# ---------------------------------------------------------------------
st.divider()
st.subheader("🔎 Pré-visualização (LIMIT 50)")
st.caption("Mostra 50 linhas da tabela selecionada, se existir na base DuckDB.")

if st.button("Exibir amostra da tabela", use_container_width=True):
    try:
        df = query(f"SELECT * FROM {table_name} LIMIT 50")
        if df.empty:
            st.warning("A tabela existe, mas não retornou linhas (ou está vazia).")
        else:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(
            "Não foi possível consultar a tabela. Verifique se ela já foi preparada/carregada."
        )
        st.exception(e)

# ---------------------------------------------------------------------
# Rodapé
# ---------------------------------------------------------------------
st.divider()
st.caption(
    f"Observações: leitura em chunks com separador `;`, tolerante a arquivo interno sem extensão; "
    f"conversão para Parquet e carga no DuckDB. Limite de upload configurado para {UPLOAD_LIMIT_MB} MB."
)