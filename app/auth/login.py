# app/auth/login.py
import streamlit as st
from app.auth.auth_utils import get_connection, get_user_by_username, verify_password, is_admin
from app.utils.streamlit_compat import force_rerun

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
            # tentar forçar rerun para atualizar UI (compatível com versões recentes)
            force_rerun()
        return

    # Layout centralizado: três colunas, conteúdo no centro
    left, center, right = st.columns([1, 2, 1])
    with center:
        st.markdown(
            "<div style='text-align:center; margin-top: 8px; margin-bottom: 8px;'>"
            "<h3 style='margin:0;'>🔐 Login</h3></div>",
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
                            # força recarregamento de forma compatível com a API atual do Streamlit
                            force_rerun()
                            return
                        else:
                            st.error("Usuário ou senha incorretos.")
                    except Exception as e:
                        st.error(f"Erro ao validar credenciais: {str(e)}")
                        st.stop()

                # Se ainda não autenticado, interrompe o app aqui
                if not st.session_state.get("authenticated", False):
                    st.stop()