from streamlit.testing.v1 import AppTest
import os

at = AppTest.from_file("tests/test1.py")
at.secrets["XATA_API_KEY"] = os.environ.get("XATA_API_KEY")
at.secrets["XATA_API_SECRET"] = os.environ.get("XATA_DB_URL")
