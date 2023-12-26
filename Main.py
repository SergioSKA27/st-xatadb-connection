import streamlit as st
from src.st_xatadb_connection import XataConnection
from streamlit_profiler import Profiler
import pandas as pd

if 'xata' not in st.session_state:
    st.session_state.xata =  st.connection('xata',type=XataConnection,table_names=['Alumno'])




with Profiler():
    st.write(st.session_state.xata.query('Alumno'))

    st.write(st.session_state.xata.Alumno.query())

    st.write(pd.DataFrame(st.session_state.xata.Alumno.schema))
