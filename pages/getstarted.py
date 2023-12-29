import streamlit as st

st.title('Getting Started with st_xatadb_connection')

st.markdown('''
## 1. Set up your Xata.io and Streamlit Environment


- Create a Xata.io account and database.

- Install st_xata_connection by running pip install st-xata-connection.

- Create a new Streamlit app by running streamlit new my_app.


2. Configure your Xata.io Credentials

To securely store your Xata.io API key and database URL, you can use Streamlit's secrets manager or environment variables.

Secrets Manager:

```
    XATA_API_KEY = "YOUR_XATA_API_KEY"
    DATABASE_URL = "YOUR_XATA_DATABASE_URL"
```


''',unsafe_allow_html=True)
