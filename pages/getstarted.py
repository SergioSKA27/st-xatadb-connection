import streamlit as st

st.title('Getting Started with st_xata_connection')

st.markdown('''
## 1. Set up your Xata.io and Streamlit Environment


- Create a Xata account and database at https://xata.io.

- Generate an API key for your Xata database.

- Get your Xata database URL endpoint.

- Install Streamlit by running `pip install streamlit`.

- Install st_xata_connection by running `pip install st-xata-connection`.


## 2. Configure your Xata Credentials

To securely store your Xata API key and database URL, you can use Streamlit's secrets manager or environment variables.

```
XATA_API_KEY = "YOUR_XATA_API_KEY"
DATABASE_URL = "YOUR_XATA_DATABASE_URL"
```

## 3. Connect to your Xata.io Database

Import the `st_xatadb_connection` package and use the st.connection() function to connect to your Xata database.


''',unsafe_allow_html=True)

st.code("""
import streamlit as st
from st_xata_connection import XataConnection

xata = st.connection('xata', type=XataConnection)

""",language='python')


st.markdown('''
## 4. Query your Xata.io Database

Use the `xata.query()` function to query your Xata.io database.
''')

st.code("""
results = xata.query("Table_Name")
""",language='python')


st.markdown('''
## 5. Display your Query Results

Use Streamlit functions like `st.write()`, `st.table()`,`st.json()`, or `st.dataframe()` to display your query results in your Streamlit app.
''')

st.code("""
st.write(results)
""",language='python')

st.markdown('''
## 6. Insert, Update, and Delete Data

You can also use st_xatadb_connection to insert, update, and delete data in your Xata.io database.
''')

st.code("""
record = {
    "name": "John Doe",
    "age": 30,
    "email": "a@b.com"
}

response = xata.insert("Table_Name", record)
""",language='python')

st.markdown('''
The `xata.insert()` function returns a response object that contains the information about the inserted record.
It looks like this:

    {
      "id": "rec_c8hnbch26un1nl0rthkg",
      "xata": {
        "version": 0,
        "createdAt": "2023-05-15T08:21:31.96526+01:00",
        "updatedAt": "2023-05-15T21:58:54.072595+01:00"
      }
    }

where `id` is the unique identifier of the inserted record, and `xata` contains the metadata about the record.
Now we can use the `id` to update the record or delete it.
''')

st.code("""

update_response = xata.update("Table_Name", "rec_c8hnbch26un1nl0rthkg", {"age": 31})
delete_response = xata.delete("Table_Name", "rec_c8hnbch26un1nl0rthkg")

""",language='python')


st.markdown('''
## 7. Working with files

Xata supports file uploads and downloads. You can use the `xata.upload_file()` function to upload a file to your Xata database
or the `xata.get_file()` function to download a file from your Xata database.

Suppose you have a file called `my_avatar.png` in your current working directory. You can upload it to your Xata database like this:
''')

st.code("""
upload_response = xata.upload_file("Table_Name", "rec_c8hnbch26un1nl0rthkg", "column_name", "my_avatar_bas64_encoded")
""",language='python')
