# pages/1_🔎_Consulta_Geral.py
import streamlit as st
from lib.loaders import query, df_to_csv_bytes, df_to_parquet_bytes
from lib.ui import inject_global_css

st.set_page_config(page_title="🔎 Consulta Geral", page_icon="🔎", layout="wide")
inject_global_css()

st.title("🔎 Consulta Geral (CNPJ / Nome / CNAE / UF / Município)")
cnpj = st.text_input("CNPJ (qualquer formato, parcial ou completo)")
nome = st.text_input("Nome Fantasia / Razão Social (contém)")
uf = st.text_input("UF", max_chars=2)
municipio = st.text_input("Município (código ou trecho do nome)")

st.markdown("#### CNAE")
colC, colD = st.columns(2)
cnae_code = colC.text_input("CNAE (código ex.: 6201501)")
cnae_desc = colD.text_input("Descrição do CNAE (contém, usa tabela de domínio)")

# Expansão de descrição -> códigos
cnae_filter = ""
params = []
if cnae_desc:
    df_codes = query("SELECT codigo FROM cnaes WHERE LOWER(descricao) LIKE ?", (f"%{cnae_desc.lower()}%",))
    codes = df_codes["codigo"].astype(str).tolist()
    if codes:
        cnae_filter = " AND (e.cnae_principal IN ({q}) OR {anysec})".format(
            q=",".join(["?"]*len(codes)),
            anysec=" OR ".join([f"POSITION(? IN COALESCE(e.cnae_sec,''))>0"]*len(codes))
        )
        params.extend(codes + codes)

sql = """
WITH est AS (
  SELECT
    CONCAT(LPAD("CNPJ BÁSICO", 8, '0'), LPAD("CNPJ ORDEM", 4, '0'), LPAD("CNPJ DV", 2, '0')) AS cnpj14,
    "NOME FANTASIA" as nome_fantasia,
    "UF" as uf, "MUNICÍPIO" as municipio,
    "CNAE FISCAL PRINCIPAL" as cnae_principal,
    "CNAE FISCAL SECUNDÁRIA" as cnae_sec
  FROM estabelecimentos
),
emp AS (
  SELECT "CNPJ BÁSICO" as cnpj_basico, "RAZÃO SOCIAL / NOME EMPRESARIAL" as razao
  FROM empresas
),
mun AS ( SELECT CAST(codigo AS TEXT) AS codigo, descricao AS municipio_nome FROM municipios ),
res AS (
  SELECT e.cnpj14, emp.razao, e.nome_fantasia, e.uf, e.municipio, mun.municipio_nome,
         e.cnae_principal, e.cnae_sec
  FROM est e
  LEFT JOIN emp ON emp.cnpj_basico = SUBSTR(e.cnpj14,1,8)
  LEFT JOIN mun ON mun.codigo = CAST(e.municipio AS TEXT)
  WHERE 1=1
)
SELECT * FROM res WHERE 1=1
"""

# filtros simples
if cnpj:
    sql += " AND res.cnpj14 LIKE ?"; params.append("%" + "".join([d for d in cnpj if d.isdigit()]) + "%")
if nome:
    sql += " AND (LOWER(res.razao) LIKE ? OR LOWER(res.nome_fantasia) LIKE ?)"
    v = f"%{nome.lower()}%"; params += [v, v]
if uf:
    sql += " AND res.uf = ?"; params.append(uf.upper())
if municipio:
    sql += " AND (CAST(res.municipio AS TEXT) LIKE ? OR LOWER(res.municipio_nome) LIKE ?)"
    params += [f"%{municipio}%", f"%{municipio.lower()}%"]
if cnae_code:
    sql += " AND (res.cnae_principal = ? OR POSITION(? IN COALESCE(res.cnae_sec,''))>0)"
    params += [cnae_code, cnae_code]
if cnae_filter:
    sql += cnae_filter  # já inclui os ? para códigos de descrição

sql += " LIMIT 5000"

if st.button("Buscar", type="primary"):
    try:
        df = query(sql, tuple(params))
        if df.empty:
            st.warning("Nenhum resultado.")
        else:
            st.success(f"{len(df)} linha(s).")
            st.dataframe(df, use_container_width=True)
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("⬇️ Exportar CSV (;)", data=df_to_csv_bytes(df),
                                   file_name="consulta_geral.csv", mime="text/csv")
            with c2:
                st.download_button("⬇️ Exportar Parquet", data=df_to_parquet_bytes(df),
                                   file_name="consulta_geral.parquet", mime="application/octet-stream")
    except Exception as e:
        st.error(f"Erro: {e}")