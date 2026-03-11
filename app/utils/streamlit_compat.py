# app/utils/streamlit_compat.py
import time
import streamlit as st
from typing import Any

def force_rerun() -> None:
    """
    Força um rerun de forma compatível:
    1) tenta st.experimental_rerun() se disponível
    2) se não, altera st.query_params (API estável) para forçar reload
    3) se não der, alterna um flag em session_state e chama st.stop()
    """
    # 1) experimental_rerun (algumas builds ainda têm)
    try:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
    except Exception:
        pass

    # 2) alterar st.query_params (API estável)
    try:
        # st.query_params é um dict-like; clonamos e atualizamos com timestamp
        params = dict(st.query_params) if hasattr(st, "query_params") else {}
        params["_rerun_ts"] = str(time.time())
        # st.query_params é settable como propriedade
        try:
            st.query_params = params
            # interrompe execução atual para que Streamlit recarregue com novos params
            st.stop()
            return
        except Exception:
            # se atribuição direta falhar, tentar via experimental_set_query_params se existir
            if hasattr(st, "experimental_set_query_params"):
                st.experimental_set_query_params(**params)
                st.stop()
                return
    except Exception:
        pass

    # 3) fallback: toggle em session_state + st.stop()
    try:
        key = "_force_rerun_flag"
        st.session_state[key] = not st.session_state.get(key, False)
        st.stop()
    except Exception:
        try:
            st.stop()
        except Exception:
            return