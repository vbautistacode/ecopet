# app/nputs.py

import streamlit as st

def show_inputs(key_prefix: str = "manual"):
    """
    Mostra formulário de inputs na sidebar.
    - key_prefix: prefixo para keys no session_state (evita colisões).
    Retorna: dict com os valores se submetido, senão None.
    """
    st.sidebar.header("📥 Inserir dados manualmente")

    with st.sidebar.form(key=f"{key_prefix}_form"):
        receita = st.number_input("Receita (R$)", min_value=0.0, value=0.0, step=100.0, format="%.2f")
        despesa = st.number_input("Despesa (R$)", min_value=0.0, value=0.0, step=100.0, format="%.2f")
        impostos = st.number_input("Impostos (R$)", min_value=0.0, value=0.0, step=50.0, format="%.2f")
        investimentos = st.number_input("Investimentos (R$)", min_value=0.0, value=0.0, step=100.0, format="%.2f")
        clientes = st.number_input("Clientes (unidades)", min_value=0, value=0, step=1, format="%d")
        caixa = st.number_input("Caixa final (R$)", min_value=0.0, value=0.0, step=100.0, format="%.2f")

        cols = st.columns([1,1])
        submit = cols[0].form_submit_button("Submeter")
        reset = cols[1].form_submit_button("Resetar")

    key_map = {
        "receita": f"{key_prefix}_receita",
        "despesa": f"{key_prefix}_despesa",
        "impostos": f"{key_prefix}_impostos",
        "investimentos": f"{key_prefix}_investimentos",
        "clientes": f"{key_prefix}_clientes",
        "caixa": f"{key_prefix}_caixa"
    }

    if reset:
        # limpa session_state keys
        for v in key_map.values():
            if v in st.session_state:
                del st.session_state[v]
        from app.utils.streamlit_compat import force_rerun
        force_rerun()

    if submit:
        # grava no session_state para reuso por outras partes do app
        st.session_state[key_map["receita"]] = float(receita)
        st.session_state[key_map["despesa"]] = float(despesa)
        st.session_state[key_map["impostos"]] = float(impostos)
        st.session_state[key_map["investimentos"]] = float(investimentos)
        st.session_state[key_map["clientes"]] = int(clientes)
        st.session_state[key_map["caixa"]] = float(caixa)

        return {
            "receita": float(receita),
            "despesa": float(despesa),
            "impostos": float(impostos),
            "investimentos": float(investimentos),
            "clientes": int(clientes),
            "caixa": float(caixa)
        }

    # nenhum submit: retornar valores já salvos (se existir) para manter formulário controlado/reativo
    existing = {}
    found = False
    for k, sk in key_map.items():
        if sk in st.session_state:
            existing[k] = st.session_state[sk]
            found = True
    return existing if found else None