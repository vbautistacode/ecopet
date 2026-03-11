# app/auth/login.py
import time
import streamlit as st
from app.auth.auth_utils import get_connection, get_user_by_username, verify_password, is_admin

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
            # forçar rerun simples via query params
            try:
                params = dict(st.query_params)
                params["_logout_ts"] = str(time.time())
                st.query_params = params
                st.stop()
            except Exception:
                # fallback: toggle flag e stop
                st.session_state["_force_rerun_flag"] = not st.session_state.get("_force_rerun_flag", False)
                st.stop()
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

                # Processamento da submissão: apenas altera estado e sinaliza sucesso
                if submit:
                    if not username or not password:
                        st.error("Preencha usuário e senha.")
                    else:
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
                                # Atualiza estado de sessão imediatamente
                                st.session_state["authenticated"] = True
                                st.session_state["role"] = user.get("role")
                                # sinaliza que devemos forçar rerun fora do contexto do form
                                st.session_state["_login_rerun_pending"] = True
                                st.success(f"Bem-vindo {user.get('name', username)}!")
                            else:
                                st.error("Usuário ou senha incorretos.")
                        except Exception as e:
                            st.error(f"Erro ao validar credenciais: {str(e)}")

        # Fora do contexto do form: se o login acabou de ocorrer, forçar rerun uma vez
        if st.session_state.get("_login_rerun_pending"):
            # remove a flag antes de forçar rerun para evitar loops
            st.session_state.pop("_login_rerun_pending", None)
            # força recarregamento via st.query_params (API estável) e interrompe execução atual
            try:
                params = dict(st.query_params)
                params["_login_ts"] = str(time.time())
                st.query_params = params
                st.stop()
                return
            except Exception:
                # fallback: toggle flag em session_state e stop
                st.session_state["_force_rerun_flag"] = not st.session_state.get("_force_rerun_flag", False)
                try:
                    st.stop()
                    return
                except Exception:
                    pass

        # Se ainda não autenticado, interrompe o app aqui
        if not st.session_state.get("authenticated", False):
            st.stop()