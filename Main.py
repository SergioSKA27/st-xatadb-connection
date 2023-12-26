import streamlit as st
from src import st_xatadb_connection as xata


con = st.connection('xata',type=xata.XatadbConnection,table_names=['Alumno'])


st.write(con.query('Alumno'))

st.write(con.Alumno.query())

con()
