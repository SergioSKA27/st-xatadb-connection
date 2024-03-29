# st-xatadb-connection
## Xata Data Base Connection to Streamlit

To further enhance the synergy between Xata and Streamlit, I've created the st_xatadb_connection package.
This package provides a seamless bridge between the two tools, making it incredibly easy to connect your Streamlit application to your Xata database.With just a few lines of code, you can perform CRUD operations (create, retrieve, update, delete) on your Xata data, generate reports and visualizations, and even interact with your data in real time.
The st_xatadb_connection package takes care of all the heavy lifting, allowing you to focus on building your application's functionality.

📄 **Package documentation**: 
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://st-xatadb-connection-docs.streamlit.app/)


🌐 **Learn More: [Xata Docs](https://xata.io/docs)** 

## **Why Use st-xatadb-connection :thinking:?**

1. 🔄 **Seamless Integration:**
   - Effortlessly connect and interact with your Xata database using intuitive methods.

2. 🛠️ **Comprehensive Functionality:**
   - Enjoy a rich set of functions, from basic queries to advanced AI interactions and file handling.

3. 🔒 **Secure Credentials Handling:**
   - Safely manage your Xata API key and database URL using Streamlit's secrets manager or environment variables.

4. 🚀 **Boosted Productivity:**
   - Streamline your workflow with efficient data manipulation and advanced search capabilities.

5. 🌐 **Real-time Connectivity:**
   - Experience real-time interactions with your Xata database, keeping your data up-to-date.

6. 🧠 **AI Integration Made Easy:**
   - Pose questions to AI models effortlessly and engage in dynamic conversations with askai and askai_follow_up functions.

7. 📄 **File Handling Simplified:**
   - Manage files seamlessly, including uploads, retrievals, and deletions with dedicated functions.

8. 📈 **Data Aggregation and Summarization:**
   - Aggregate and summarize data effortlessly, gaining insights at your fingertips.

9. 🌟 **Advanced Schema Management:**
   - Dynamically create, delete, and modify tables and columns with ease.

10. ⚡️ **Efficient Bulk Operations:**
    - Leverage bulk processing and transactions for lightning-fast database operations.

11. 🖼️ **Image Transformation Capabilities:**
    - Transform images on-the-fly with the image_transform function.

12. 🌐 **Pagination for Large Datasets:**
    - Navigate through large datasets seamlessly with next_page and prev_page functions.

13. 🚀 **Developer-Friendly:**
    - Designed for developers by developers, ensuring a smooth and intuitive experience.



## Features
🔍 **Data Manipulation Made Easy**
<details>
<summary>methods</summary>

<ul>
    <li><strong>query:</strong> Execute queries on specific tables with precision.</li>
    <li><strong>get:</strong> Retrieve records using record IDs.</li>
    <li><strong>insert:</strong> Seamlessly insert records into tables.</li>
    <li><strong>upsert:</strong> Replace records with ease.</li>
    <li><strong>update:</strong> Effortlessly update records in your tables.</li>
    <li><strong>delete:</strong> Remove records effortlessly.</li>
</ul>

</details>

🔎 **Advanced Search Capabilities**
<details>
<summary>methods</summary>
<ul>
    <li><strong>search:</strong> Discover specific queries in a branch.</li>
    <li><strong>search_on_table:</strong> Uncover data in a table using a search query.</li>
    <li><strong>vector_search:</strong> Harness the power of vector searches.</li>
</ul>
</details>

📊 **Data Aggregation and Summarization**
<details>
<summary>methods</summary>
<ul>
    <li><strong>aggregate:</strong> Aggregate data based on queries.</li>
    <li><strong>summarize:</strong> Summarize data in tables with precision.</li>
</ul>
</details>

🔄 **Transaction Management**
<details>
<summary>methods</summary>
<ul>
    <li><strong>transaction:</strong> Perform transactions effortlessly.</li>
    <li><strong>sql_query:</strong> Execute SQL queries seamlessly.</li>
</ul>
</details>

🧠 **AI Integration**
<details>
<summary>methods</summary>
<ul>
    <li><strong>askai:</strong> Pose questions and receive AI-driven responses.</li>
    <li><strong>askai_follow_up:</strong> Engage in dynamic conversations with AI models.</li>
</ul>
</details>

📁 **File Handling Made Simple**
<details>
<summary>methods</summary>
<ul>
    <li><strong>upload_file:</strong> Upload files to specific tables and records.</li>
    <li><strong>get_file:</strong> Retrieve files with ease.</li>
    <li><strong>delete_file:</strong> Effortlessly remove files from your database.</li>
</ul>
</details>

🖼️ **Image Transformation**
<details>
<summary>methods</summary>
<ul>
    <li><strong>image_transform:</strong> Transform images effortlessly.</li>
</ul>
</details>

📄 **Pagination and Schema Management**
<details>
<summary>methods</summary>
<ul>
    <li><strong>next_page:</strong> Retrieve the next page of results.</li>
    <li><strong>prev_page:</strong> Access the previous page of results.</li>
    <li><strong>get_schema:</strong> Explore table schemas dynamically.</li>
</ul>
</details>

🛠️ **Database Administration**
<details>
<summary>methods</summary>
<ul>
    <li><strong>create_table:</strong> Create new tables effortlessly.</li>
    <li><strong>delete_table:</strong> Remove tables with precision.</li>
    <li><strong>create_column:</strong> Add new columns seamlessly.</li>
    <li><strong>delete_column:</strong> Effortlessly remove columns.</li>
    <li><strong>get_columns:</strong> Retrieve table columns dynamically.</li>
</ul>
</details>

🚀 **Bulk Operations and API Requests**
<details>
<summary>methods</summary>
<ul>
    <li><strong>bulk_processor:</strong> Facilitate bulk processing effortlessly.</li>
    <li><strong>bulk_transaction:</strong> Manage bulk transactions seamlessly.</li>
</ul>
</details>


## Getting Started with st_xata_connection

## 1. Set up your Xata.io and Streamlit Environment

- Create a Xata account and database at [Xata Web](https://xata.io).

- Generate an API key for your Xata database.

- Get your Xata database URL endpoint.

- Install Streamlit by running `pip install streamlit`.

- Install st_xata_connection by running `pip install st-xatadb-connection`.

## 2. Configure your Xata Credentials

To securely store your Xata API key and database URL, you can use Streamlit's [secrets](https://docs.streamlit.io/library/advanced-features/secrets-management) manager or environment variables.

``` toml
XATA_API_KEY = "YOUR_XATA_API_KEY"

XATA_DB_URL = "YOUR_XATA_DATABASE_URL"
```

## 3. Connect to your Xata.io Database

Import the `st_xata_connection` package and use the st.connection() function to connect to your Xata database.
You could also use the the `st.session_state` object to store the connection object and reuse it across your Streamlit app.

``` python
import streamlit as st
from st_xatadb_connection import XataConnection

xata = st.connection('xata', type=XataConnection)
```

## 4. Query your Xata.io Database

Use the `xata.query()` function to query your Xata.io database.

``` python
results = xata.query("Table_Name")
```

## 5. Display your Query Results

Use Streamlit functions like `st.write()`, `st.table()`,`st.json()`, or `st.dataframe()` to display your query results in your Streamlit app.

```python
st.write(results)
```

## 6. Insert, Update, and Delete Data

You can also use st_xatadb_connection to insert, update, and delete data in your Xata.io database.

``` python
record = {
    "name": "John Doe",
    "age": 30,
    "email": "a@b.com"
}
response = xata.insert("Table_Name", record)
```

The `xata.insert()` function returns a response object that contains the information about the inserted record.
It looks like this:

```json
{
      "id": "rec_c8hnbch26un1nl0rthkg",
      "xata": {
        "version": 0,
        "createdAt": "2023-05-15T08:21:31.96526+01:00",
        "updatedAt": "2023-05-15T21:58:54.072595+01:00"
      }
}
```

where `id` is the unique identifier of the inserted record, and `xata` contains the metadata about the record.
Now we can use the `id` to update the record or delete it. And in the same way, these functions also return a response object
that contains the information about the updated or deleted record.

```python
update_response = xata.update("Table_Name", "rec_c8hnbch26un1nl0rthkg", {"age": 31})
delete_response = xata.delete("Table_Name", "rec_c8hnbch26un1nl0rthkg")
```

## 7. Working with files

Xata supports file uploads and downloads. You can use the `xata.upload_file()` function to upload a file to your Xata database
or the `xata.get_file()` function to download a file from your Xata database.

Suppose you have a file called `my_avatar.png` in your current working directory. You can upload it to your Xata database like this:

```python
upload_response = xata.upload_file("Table_Name", "rec_c8hnbch26un1nl0rthkg", "column_name", "my_avatar_bas64_encoded")
```

Now you can download the file from your Xata database like this:

```python
download_response = xata.get_file("Table_Name", "rec_c8hnbch26un1nl0rthkg", "column_name")
```

## 8. Transactions

If you want to perform multiple operations on your Xata database as a single unit of work, you can use transactions.
For this, you can use the `xata.transaction()` function. It takes a list of operations as an argument and returns a response object.

Suppose you want to insert a record and update another record in your Xata database as a single unit of work.
You can do it like this:

```python
transaction_response = xata.transaction([
{"insert": {"table": "Table_Name", "record": {"name": "Marie Doe", "age": 21, "email": "marie@mail.com"}}}
{"update": {"table": "Table_Name", "id": "rec_c8hnbch26un1nl0rthkg", "fields": {"age": 31}}}
{"get": {"table": "Table_Name", "id": "rec_c8hnbch26un1nl0rthkg","columns": ["name", "age"]}}
{"delete": {"table": "Table_Name", "id": "rec_c8hnbch26un1nl0rthkg"}}
])
```

Note that the `transaction()` function takes a list of operations as an argument. Each operation is a dictionary that contains
the type of operation and the operation-specific arguments. The supported operations are `insert`, `update`, `get`, and `delete`.

## 9. SQL Queries

If you feel more comfortable with SQL, you can use the `xata.sql_query()` function to query your Xata database using SQL.

The `xata.sql_query()` function takes a SQL query as an argument and returns a response object with the query results.
For example, suppose you want to query your Xata database using SQL. You can do it like this:

```python
sql_response = xata.sql_query("SELECT * FROM Table_Name")
```

## 10. Asking to the AI Assistant

Xata has an AI assistant that can help you with your queries. You can use the `xata.askai()` function to ask a question to the AI assistant,
taking a reference to the table you want to query and the question you want to ask as arguments.

Suppose you want to ask the AI assistant to find the people in your table whose age is greater than 30. You can do it like this:

```python
response = xata.askai("Table_Name", "Find the people whose age is greater than 30")
```

The response looks like this:

```json
{
  "answer": "< answer >",
  "sessionId": "cg52bk1eqh5rd5hndhq95jercs",
  "records": [
    "b70d541d114ff54ad15915636450663f",
    "8ae4837002e21f013aa85c30a126ea1c",
    "4b137344a3c53d5152c45ed514188cd2"
  ]
}
```

---

The `st_xata_connection` package is a powerful tool that makes it easy to connect your Streamlit app to your Xata database.
In this article, we've covered the basics of using `st_xata_connection` to query your Xata database, insert, update, and delete data,
work with files, perform transactions, and ask questions to the AI assistant. But there's a lot more you can do with `st_xata_connection`.
