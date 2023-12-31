import streamlit as st
from st_xata_connection import XataConnection

# Create a connection to your Xata database
# To test this you need to create a Xata account and a database and create a table called Users
#This test only connect to the database and make a query to the table

conn = st.connection(name="Xata-Test",type=XataConnection)

response = conn.query("Users")

st.write(response)
