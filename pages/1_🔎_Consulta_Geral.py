import streamlit as st
from lib.loaders import query
from lib.util import mask_cnpj

st.title("🔎 Consulta Geral (por CNPJ/Nome/CNAE/UF/Município)")
cnpj = st.text_input("CNPJ (qualquer formato)")
nome = st.text_input("Nome Fantasia / Razão Social (contém)")
cnae = st.text_input("CNAE (principal ou secundário, ex.: 6201501)")
uf = st.text_input("UF", max_chars=2)
municipio = st.text_input("Município (código ou texto)")

sql = """
WITH est AS (
  SELECT
    CONCAT(LPAD("CNPJ BÁSICO", 8, '0'),
           LPAD("CNPJ ORDEM", 4, '0'),
           LPAD("CNPJ DV", 2, '0')) AS cnpj14,
    "NOME FANTASIA" as nome_fantasia,
    "UF" as uf,
    "MUNICÍPIO" as municipio,
    "CNAE FISCAL PRINCIPAL" as cnae_principal,
    "CNAE FISCAL SECUNDÁRIA" as cnae_sec
  FROM estabelecimentos
),
emp AS (
  SELECT "CNPJ BÁSICO" as cnpj_basico, "RAZÃO SOCIAL / NOME EMPRESARIAL" as razao
  FROM empresas
)
SELECT
  e.cnpj14,
  emp.razao,
  e.nome_fantasia,
  e.uf, e.municipio,
  e.cnae_principal, e.cnae_sec
FROM est e
LEFT JOIN emp ON emp.cnpj_basico = SUBSTR(e.cnpj14,1,8)
WHERE 1=1
"""
params = []
if cnpj:
    sql += " AND e.cnpj14 LIKE ?"; params.append(f"%{''.join([d for d in cnpj if d.isdigit()])}%")
if nome:
    sql += " AND (LOWER(emp.razao) LIKE ? OR LOWER(e.nome_fantasia) LIKE ?)"
    v = f"%{nome.lower()}%"; params += [v, v]
if cnae:
    sql += " AND (e.cnae_principal = ? OR POSITION(? IN COALESCE(e.cnae_sec,'')) > 0)"
    params += [cnae, cnae]
if uf:
    sql += " AND e.uf = ?"; params.append(uf.upper())
if municipio:
    sql += " AND CAST(e.municipio AS TEXT) LIKE ?"; params.append(f"%{municipio}%")

sql += " LIMIT 1000"

if st.button("Buscar", type="primary"):
    try:
        df = query(sql, tuple(params))
        if not df.empty:
            df["CNPJ"] = df["cnpj14"].map(mask_cnpj)
            st.dataframe(df.drop(columns=["cnpj14"]), use_container_width=True)
        else:
            st.warning("Nenhum resultado.")
    except Exception as e:
        st.error(f"Erro: {e}")