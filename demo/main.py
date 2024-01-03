import streamlit as st
from st_xatadb_connection import XataConnection




st.title('Demo Xata Connection')
# Create a connection to your Xata database
xata = st.connection('xata',type=XataConnection)

st.code('''
import streamlit as st
from st_xatadb_connection import XataConnection

# Create a connection to your Xata database
xata = st.connection('xata',type=XataConnection)
''')

st.subheader('Basic query form')
# Create a query to your Xata database
result = xata.query('Users')
st.write('Create a query to your Xata database, this is the most basic query.')
st.code('result = xata.query("Users")')
# Show the result
st.write('Show the result of the query.')
st.code('st.write(result)')
st.write(result)


st.subheader('Schema of the table')
# Show the schema of the table
st.write('Show the schema of the table.')
st.write(xata.get_schema('Users'))

# insert a new record
st.header('Insert a new record')
st.subheader('read the data from the user')
#Read the data from the user
name = st.text_input('Name')
birth_date = st.date_input('Birth date')
username = st.text_input('Username')
email = st.text_input('Email')

# Save the data in a dictionary
record = {'name':name,'birthday':birth_date.strftime("%Y-%m-%dT%H:%M:%SZ"),'username':username,'email':email}

# Insert the record in the table
st.write('Insert the record in the table. You need to provide the table name and the record.')
st.code("record = {'name':name,'birthday':birth_date.strftime('%Y-%m-%dT%H:%M:%SZ'),'username':username,'email':email}")
st.code('insert_result = xata.insert("Users",record)')
if st.button('Insert'):
    with st.spinner('Inserting data...'):
        insert_result = xata.insert('Users',record)

    # Show the result
    st.write(insert_result)

st.header('Get a record by id')
# Get a record by id
record_id = st.text_input('Record id')
st.code('get_result = xata.get("Users",record_id)')
if st.button('Get by id'):
    with st.spinner('Getting data...'):
        get_result = xata.get('Users',record_id)
        # Show the result
        st.write(get_result)
# Update a record
st.header('Update a record')
st.write('Update a record. You need to provide the record id and the data to update.')
st.code('update_result = xata.update("Users",record_id,{"email":email_update})')

record_id_update = st.text_input('Record id to update')
email_update = st.text_input('Email to update')

if st.button('Update'):
    with st.spinner('Updating data...'):
        update_result = xata.update('Users',record_id_update,{'email':email_update})
        # Show the result
        st.write(update_result)

# Delete a record
st.header('Delete a record')
st.write('Delete a record. You need to provide the record id.')

record_id_delete = st.text_input('Record id to delete')
st.code('delete_result = xata.delete("Users",record_id)')
if st.button('Delete'):
    with st.spinner('Deleting data...'):
        delete_result = xata.delete('Users',record_id_delete)
        # Show the result
        st.write(delete_result)
