# lib/ui.py
import streamlit as st

def inject_global_css():
    st.markdown("""
    <style>
    /* Títulos mais próximos da identidade CAIXA */
    h1, h2, h3, h4 { letter-spacing: 0.2px; }
    .stMetric { border-radius: 10px; }
    /* Botões mais encorpados */
    .stButton>button { border-radius: 8px; padding: 0.6rem 0.9rem; font-weight: 600; }
    /* Dataframe borda suave */
    .stDataFrame { border: 1px solid #E6EBF2; border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)