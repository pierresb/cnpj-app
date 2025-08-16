# pages/7_📄_Relatório_do_CNPJ.py
import re
import streamlit as st
from lib.loaders import query, df_to_csv_bytes, df_to_parquet_bytes
from lib.ui import inject_global_css

st.set_page_config(page_title="📄 Relatório do CNPJ", page_icon="📄", layout="wide")
inject_global_css()

st.title("📄 Relatório Consolidado do CNPJ")

cnpj_in = st.text_input("Informe o CNPJ (com ou sem máscara)")
digits = re.sub(r"\D+","", cnpj_in or "")

if st.button("Gerar Relatório", type="primary", disabled=not digits):
    if len(digits) < 8:
        st.error("Informe ao menos o CNPJ BÁSICO (8 dígitos).")
    else:
        cnpj_basico = digits[:8]

        # Empresa
        emp = query("""
            SELECT
              "CNPJ BÁSICO" AS cnpj_basico,
              "RAZÃO SOCIAL / NOME EMPRESARIAL" AS razao_social,
              "NATUREZA JURÍDICA" AS natureza,
              "CAPITAL SOCIAL DA EMPRESA" AS capital_social,
              "PORTE DA EMPRESA" AS porte,
              "ENTE FEDERATIVO RESPONSÁVEL" AS efr
            FROM empresas
            WHERE "CNPJ BÁSICO" = ?
        """, (cnpj_basico,))

        # Domínio da natureza
        nat = query("""SELECT descricao AS natureza_nome FROM naturezas WHERE CAST(codigo AS TEXT)=CAST(? AS TEXT) LIMIT 1""",
                    (emp.iloc[0]["natureza"],)) if not emp.empty else None

        # Estabelecimentos
        est = query("""
            WITH E AS (
              SELECT "CNPJ BÁSICO" AS cnpj_basico, "CNPJ ORDEM" AS ordem, "CNPJ DV" AS dv,
                     "NOME FANTASIA" AS nome_fantasia, "UF" AS uf, "MUNICÍPIO" AS municipio,
                     "PAIS" AS pais_cod, "SITUAÇÃO CADASTRAL" AS situacao, "DATA SITUAÇÃO CADASTRAL" AS data_sit,
                     "CNAE FISCAL PRINCIPAL" AS cnae_principal, "CNAE FISCAL SECUNDÁRIA" AS cnae_sec
              FROM estabelecimentos WHERE "CNPJ BÁSICO" = ?
            ),
            M AS (SELECT CAST(codigo AS TEXT) AS codigo, descricao AS municipio_nome FROM municipios),
            P AS (SELECT CAST(codigo AS TEXT) AS codigo, descricao AS pais_nome FROM paises)
            SELECT
              CONCAT(LPAD(E.cnpj_basico,8,'0'), LPAD(E.ordem,4,'0'), LPAD(E.dv,2,'0')) AS cnpj14,
              E.nome_fantasia, E.uf, E.municipio, M.municipio_nome, E.pais_cod, P.pais_nome,
              E.situacao, E.data_sit, E.cnae_principal, E.cnae_sec
            FROM E
            LEFT JOIN M ON M.codigo = CAST(E.municipio AS TEXT)
            LEFT JOIN P ON P.codigo = CAST(E.pais_cod AS TEXT)
        """, (cnpj_basico,))

        # Sócios
        socios = query("""
            SELECT
              "IDENTIFICADOR DE SÓCIO" AS ident_socio,
              "NOME DO SÓCIO (NO CASO PF) OU RAZÃO SOCIAL (NO CASO PJ)" AS nome_razao,
              "CNPJ/CPF DO SÓCIO" AS doc,
              "QUALIFICAÇÃO DO SÓCIO" AS qualif,
              "DATA DE ENTRADA SOCIEDADE" AS data_entrada_soc,
              "FAIXA ETÁRIA" AS faixa_etaria
            FROM socios WHERE "CNPJ BÁSICO" = ?
        """, (cnpj_basico,))

        # Simples
        simples = query("""
            SELECT
              "OPÇÃO PELO SIMPLES" AS opcao_simples, "DATA DE OPÇÃO PELO SIMPLES" AS dt_op_simples,
              "DATA DE EXCLUSÃO DO SIMPLES" AS dt_exc_simples,
              "OPÇÃO PELO MEI" AS opcao_mei, "DATA DE OPÇÃO PELO MEI" AS dt_op_mei,
              "DATA DE EXCLUSÃO DO MEI" AS dt_exc_mei
            FROM simples WHERE "CNPJ BÁSICO" = ?
        """, (cnpj_basico,))

        # Render
        if emp.empty:
            st.warning("CNPJ Básico não encontrado na tabela de Empresas. Verifique se os dados foram carregados.")
        else:
            st.subheader("🏢 Empresa")
            c1, c2, c3 = st.columns(3)
            c1.metric("Razão Social", emp.iloc[0]["razao_social"])
            c2.metric("Natureza Jurídica", (nat.iloc[0]["natureza_nome"] if nat is not None and not nat.empty else emp.iloc[0]["natureza"]))
            c3.metric("Porte", emp.iloc[0]["porte"])
            st.caption(f"EFR: {emp.iloc[0]['efr']}")
            st.caption(f"Capital Social: {emp.iloc[0]['capital_social']}")

            st.subheader("🏬 Estabelecimentos")
            st.dataframe(est, use_container_width=True)

            st.subheader("👥 Sócios")
            st.dataframe(socios, use_container_width=True)

            st.subheader("💡 Simples/MEI")
            st.dataframe(simples, use_container_width=True)

            # Exports agrupados
            st.markdown("### ⬇️ Exportações")
            colX, colY = st.columns(2)
            with colX:
                payload = {
                    "empresa.csv": df_to_csv_bytes(emp),
                    "estabelecimentos.csv": df_to_csv_bytes(est),
                    "socios.csv": df_to_csv_bytes(socios),
                    "simples.csv": df_to_csv_bytes(simples),
                }
                # zip “manual” simples
                import io, zipfile
                buff = io.BytesIO()
                with zipfile.ZipFile(buff, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for name, data in payload.items():
                        zf.writestr(name, data)
                st.download_button("Baixar ZIP (CSVs)", data=buff.getvalue(), file_name=f"relatorio_{cnpj_basico}.zip", mime="application/zip")
            with colY:
                # parquet único (empresa repetida; prática: separar, mas vamos oferecer uma visão única)
                import pandas as pd
                combined = {
                    "empresa": emp.assign(_table="empresa"),
                    "estabelecimentos": est.assign(_table="estabelecimentos"),
                    "socios": socios.assign(_table="socios"),
                    "simples": simples.assign(_table="simples"),
                }
                big = pd.concat(combined.values(), ignore_index=True)
                st.download_button("Baixar Parquet (consolidado)", data=df_to_parquet_bytes(big),
                                   file_name=f"relatorio_{cnpj_basico}.parquet", mime="application/octet-stream")