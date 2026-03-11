# app/auth/login.py
import time
import streamlit as st
from app.auth.auth_utils import get_connection, get_user_by_username, verify_password, is_admin

def _force_rerun():
    """
    Tenta forçar um rerun de forma compatível com várias versões do Streamlit.
    1) tenta st.experimental_rerun()
    2) se não existir, altera query params (isso provoca rerun)
    3) se tudo falhar, apenas retorna (o app continuará sem crash)
    """
    try:
        # preferencial: API direta (pode não existir em alguns builds)
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
    except Exception:
        # se a chamada existir mas falhar, seguimos para fallback
        pass

    try:
        # fallback: alterar query params força um rerun no Streamlit
        # usamos um timestamp para garantir mudança
        params = st.experimental_get_query_params() if hasattr(st, "experimental_get_query_params") else {}
        params["_login_rerun"] = str(time.time())
        if hasattr(st, "experimental_set_query_params"):
            st.experimental_set_query_params(**params)
            # interrompe a execução atual para que o Streamlit recarregue
            st.stop()
            return
    except Exception:
        pass

    # último recurso: nada mais a fazer; o caller continuará normalmente
    return


def show_login():
    # Inicializa estado
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
        st.session_state["role"] = None

    # Se já autenticado, mostra logout e sai da função
    if st.session_state["authenticated"]:
        st.sidebar.success(f"Perfil: {str(st.session_state.get('role', '')).capitalize()}")
        if st.sidebar.button("Logout"):
            st.session_state["authenticated"] = False
            st.session_state["role"] = None
            # tentar forçar rerun para atualizar UI
            _force_rerun()
        return

    # Layout centralizado: três colunas, conteúdo no centro
    left, center, right = st.columns([1, 2, 1])
    with center:
        st.markdown(
            "<div style='text-align:center; margin-top: 8px; margin-bottom: 8px;'>"
            "<h2 style='margin:0;'>🔐 Login</h2></div>",
            unsafe_allow_html=True,
        )

        # Formulário para inputs menores e alinhados
        with st.form(key="login_form"):
            c1, c2, c3 = st.columns([1, 2, 1])
            with c2:
                username = st.text_input("Usuário", key="login_username")
                password = st.text_input("Senha", type="password", key="login_password")

            submit = st.form_submit_button("Entrar")

    # Se o usuário submeteu o formulário, processar autenticação
    if submit:
        if not username or not password:
            st.error("Preencha usuário e senha.")
            st.stop()

        try:
            with st.spinner("Validando credenciais..."):
                conn = get_connection()
                try:
                    user = get_user_by_username(conn, username)
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

            if user and verify_password(password, user.get("password_hash")):
                st.session_state["authenticated"] = True
                st.session_state["role"] = user.get("role")
                st.success(f"Bem-vindo {user.get('name', username)}!")
                # força recarregamento de forma compatível
                _force_rerun()
                # se _force_rerun não interromper, apenas retorna para continuar execução
                return
            else:
                st.error("Usuário ou senha incorretos.")
        except Exception as e:
            st.error(f"Erro ao validar credenciais: {str(e)}")
            st.stop()

    # Se ainda não autenticado, interrompe o app aqui
    if not st.session_state.get("authenticated", False):
        st.stop()