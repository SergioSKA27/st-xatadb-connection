import streamlit as st
from src.st_xatadb_connection import XataConnection
from streamlit_profiler import Profiler




st.set_page_config(layout="wide")
if 'xata' not in st.session_state:
    st.session_state.xata =  st.connection('xata',type=XataConnection,table_names=['Alumno'])
xata = st.session_state.xata




st.markdown('<h1> Streamlit Xata Data Base Connection <img src="https://xata.io/icon.svg?9d7a66ec4c0ad6b1" width="50" height="50" align-items="center"/></h1>',
unsafe_allow_html=True)
st.caption('By: Sergio Demis Lopez Martinez')
st.divider()
'''

## What is Xata?

### Serverless data platform

Xata is a new type of Cloud service: it combines multiple types of stores (relational database, search engine, analytics engine)
into a single service. The combined functionality is exposed over a single consistent API.
It is also vertically integrated: an advanced admin UI and high-level SDKs come into the package.

:blue[They call this type of service a Serverless Data Platform.]

'''

st.image('https://xata.io/_next/image?url=%2Fmdx%2Fdocs%2F030-Concepts%2Fserverless-data-platform.png&w=1920&q=75',width=600)

'''
This type of service has several benefits:

- it simplifies building applications, thanks to its vertical integration and serverless capabilities

- it offers more data-related functionality than a typical database, for example, free-text-search with relevancy controls,
and more advanced analytics

- it grows with you, in both scale and required functionality because it is based on proven data technologies,
each best-in-class for the problems they solve.

Currently, the Xata service uses PostgreSQL as the data store, Elasticsearch as the search/analytics engine,
and Kafka for storing the logical replication events. It also offers support for edge-caching.

In the future, they plan to add built-in support for centralized caching (for example, Redis), block storage,
better support for sharding between DBs, automatic global distribution, and so on.

This type of data architecture, where a relational DB is used as the primary store and the data in it is replicated
in other more specialized stores (search engine, data warehousing, business intelligence systems, etc.)
is very popular among companies over a certain size. However, it requires significant amounts of glue code,
operational overhead, monitoring, and expertise. Not anymore, Xata packages it all in a single opinionated service.
'''

with Profiler():
    st.write(xata.query('Alumno'))
