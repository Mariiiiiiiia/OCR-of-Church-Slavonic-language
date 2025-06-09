import streamlit as st
import requests

st.title("Сервис автоматического распознавания церковнославянских текстов")
st.subheader("Загрузка ZIP архива")

# Инициализация session_state
if "request_id" not in st.session_state:
    st.session_state.request_id = ""

# Переключатель: страницы или развороты
file_type = st.radio("Сканы книги представляют собой:", ["Страницы", "Развороты"])
param = 0 if file_type == "Страницы" else 1

# Окно загрузки файла
uploaded_file = st.file_uploader("Загрузите ZIP файл", type=["zip"])

# Кнопка для отправки файла (всегда видима)
if st.button("Отправить файл", key="upload_button"):
    if uploaded_file is not None:
        files = {"zip": uploaded_file}
        data = {"param": param}
        response = requests.post("http://127.0.0.1:9999/upload", files=files, data=data)
        st.session_state.request_id = response.json()["id"]
        st.success(f"Файл отправлен. ID запроса: {st.session_state.request_id}")
    else:
        st.warning("Пожалуйста, загрузите ZIP-архив перед отправкой.")

# Новый заголовок перед полем ввода ID
st.subheader("Получение результатов обработки")

# Поле для ввода ID с сохранением значения из session_state
manual_request_id = st.text_input(
    "Введите ID запроса:",
    value=st.session_state.request_id,
    key="request_id_input"
)

# Определяем какой ID использовать (введенный вручную или из session_state)
request_id_to_use = manual_request_id if manual_request_id else st.session_state.request_id

# Кнопка для получения результата (всегда видима)
if st.button("Получить результат", key="get_result_button"):
    if request_id_to_use:
        try:
            status_response = requests.get(f"http://127.0.0.1:9999/status/{request_id_to_use}")
            result = status_response.json()
            st.info(f"Прогресс: {result['progress']}")
            st.markdown(f"[📦 Скачать результат]({result['download_url']})", unsafe_allow_html=True)
        except Exception as e:
            st.info(result['message'])
    else:
        st.warning("Пожалуйста, введите ID запроса.")
