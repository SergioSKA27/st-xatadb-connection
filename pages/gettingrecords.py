import streamlit as st
from src.st_xata_connection import XataConnection

xata = st.connection('xata', type=XataConnection)

st.title('Getting records')

st.markdown('''
---
In this tutorial, you'll learn how to use st_xatadb_connection to connect to your Xata.io database and query your data.

the general form of a query is:''')

st.code("""
from st_xata_connection import XataConnection

xata = st.connection('xata', type=XataConnection)

data = xata.query("{table_name}", {
  "columns": [...],
  "filter": {
    ...
  },
  "sort": {
    ...
  },
  "page": {
    ...
  }
})
""",language='python')


st.markdown('All the requests are optional, so the simplest query request looks like this:')
st.code("""
results = xata.query("Table_Name")
""")

st.markdown('The response looks like this:')

st.code("""
{
    "records": [
        {
          "id": "rec_c8hng2h26un90p8sr7k0",
          "name": "Matrix",
          "owner": {
            "id": "myid"
          },
          "xata": {
            "version": 0,
            "createdAt": "2023-05-15T08:21:31.96526+01:00",
            "updatedAt": "2023-05-15T21:58:54.072595+01:00"
          }
        }
    ],
    "meta": {
        "page": {
            "cursor": "jMq7DcIwEIDhnjH-2sWRAsItAT2KkOU8bAgB3Zkqyu6IDei_",
            "more": false
        }
    }
}""",language='json')


st.subheader('Getting a single record by ID')

st.markdown('''
You can retrieve a record with a given ID using a request like this:
''')

st.code("""
record = xata.get("Table_Name", "my_id")
""",language='python')



st.markdown('''
## Columns Selection

By default, the Query API returns all columns of the queried table. For link columns, only the ID column of the linked
records is included in the response. You can use column selection to both reduce the number of columns returned, and to
include columns from linked tables. It's worth noting that the special columns id, xata.version, xata.createdAt and
xata.updatedAt are always returned, even if they are not explicitly requested.

For example, if you are only interested in the name and the city of the user, you can make a request like this:
''')

st.code("""
users = xata.query("Users", {
  "columns": ["name", "city"]
})
""",language='python')


st.markdown('''

## Selecting Columns from the Linked Tables

The same syntax can be used to select columns from a linked table, therefore adding new columns to the response.
For example, if you have a table called Users, and a table called Orders, and you want to retrieve the name of the user
for each order, you can make a request like this:
''')

st.code("""
results = xata.query("Orders", {"columns": ["id", "user.name"]})
""",language='python')

st.markdown('You can do this transitively as well, for example:')

st.code("""
posts = xata.query("Posts", {
  "columns": [
    "title",
    "author.*",
    "author.team.*"
  ]
})
""",language='python')
