import streamlit as st

st.set_page_config(layout="wide",
initial_sidebar_state="collapsed", page_title="Streamlit Xata Connection")

st.markdown('''
<h1 style="font-family:Courier; text-align:center;">Streamlit Xata Data Base Connection

</h1>
''',unsafe_allow_html=True)
#<img src="https://streamlit.io/images/brand/streamlit-logo-primary-colormark-darktext.svg" width="100" height="50" align-items="center"/>

st.caption('By: Sergio Demis Lopez Martinez')
st.divider()

st.markdown('''
<h1 style="font-family:Courier; text-align:center;padding-bottom:0px;">Xata and Streamlit </h1>
<h2 style="font-family:Courier; text-align:center;">A Dynamic Duo for Building Powerful Web Applications</h2>
''', unsafe_allow_html=True)

st.markdown('''
<div style="text-align:center">
In the realm of web development, the combination of Xata and Streamlit is nothing short of a dream team. Xata,
with its schemaless relational SQL database and simple REST API, provides a flexible and scalable foundation for storing
and managing data. Streamlit, on the other hand, is an open-source framework that empowers developers to create
interactive web applications with minimal effort. Together, they form a synergistic partnership that unlocks a world of
possibilities for building sophisticated web applications.
</div>


---

''' ,unsafe_allow_html=True)


st.markdown('''
### Xata: The Flexible and Scalable Data Storage Solution

<div style="text-align:justify">
Imagine a database that can effortlessly adapt to your evolving needs, without the constraints of a rigid schema.
Xata.io brings this vision to life, allowing you to add, remove, and modify columns on the fly. This flexibility makes
it an ideal choice for applications that require frequent changes or those that deal with diverse data types.
</div>

<div style="text-align:justify;padding-bottom:10px">
Furthermore, Xata's scalability ensures that your application can handle even the most demanding workloads.
Its cloud-based architecture can seamlessly scale up or down as your data grows or shrinks, ensuring optimal performance at all times.
<br>
</div>

''',unsafe_allow_html=True)


st.markdown('''

### The st_xatadb_connection Package: Simplifying the Integration Process

<div style="text-align:justify">
To further enhance the synergy between Xata and Streamlit, I've created the st_xatadb_connection package.
This package provides a seamless bridge between the two tools, making it incredibly easy to connect your Streamlit
application to your Xata database.
</div>

<div style="text-align:justify;padding-bottom:10px">
With just a few lines of code, you can perform CRUD operations (create, retrieve, update, delete) on your Xata data,
generate reports and visualizations, and even interact with your data in real time.
The st_xatadb_connection package takes care of all the heavy lifting, allowing you to focus on building your application's functionality.
</div>

##### The Simplicity of st_xatadb_connection: Building Apps with Xata and Streamlit

<div style="text-align:justify">
st_xatadb_connection is the ultimate game-changer for developers looking to build data-driven web applications with
Xata and Streamlit. Its intuitive API and powerful features make it a breeze to connect to your Xata database and
start working with your data.
</div>

With st_xatadb_connection, you can:

- Set your Xata credentials securely in Streamlit's secrets manager or environment variables.

- Connect to your Xata database with a single line of code.

- Query your Xata.io tables with ease using a familiar SQL-like syntax.

- Display your query results in Streamlit with just a few clicks.

Here's a sneak peek into how incredibly simple it is to use st_xatadb_connection:

</div>''' ,unsafe_allow_html=True)


st.code("""
import streamlit as st
from st_xata_connection import XataConnection

# Connect to your Xata.io database
xata = st.connection('xata', type=XataConnection)

# Query your 'Table' table
results = xata.query("Table")

# Display the query results in your Streamlit app
st.write(results)
""",language='python')


st.markdown('''
That's it! With just a few lines of code, you've created a fully functional web application that displays data from your Xatadatabase.

Benefits of using st_xatadb_connection:

- Effortless Setup: st_xatadb_connection's intuitive API makes it incredibly easy to set up and configure.
You can connect to your Xata.io database and start querying your data in minutes.

- Seamless Integration: st_xatadb_connection integrates seamlessly with Streamlit, allowing you to effortlessly display
your query results in interactive visualizations and dashboards.

- Accelerated Development: st_xatadb_connection's streamlined API and pre-built functions enable rapid development,
saving you valuable time and effort.

##### Unlock the Power of st_xatadb_connection Today!

<div style="text-align:justify">
st_xatadb_connection is the key to unlocking the full potential of Xata and Streamlit. With its user-friendly
interface and robust capabilities, it's the perfect tool for building data-driven web applications that are both
powerful and easy to use.
</div>

<div style="text-align:center;font-family: Courier">
Experience the simplicity and efficiency of st_xatadb_connection firsthand. Try it out today and see how effortlessly
you can build amazing web applications with Xata and Streamlit!
</div>
''' ,unsafe_allow_html=True)



st.markdown('''
---

<h1 style="text-align:center;font-family: Courier">
Xata Free is a Free Cloud Database!
</h1>

This cloud-based database offers an impressive array of features, all available for free:

- **10 Database Branches**: Xata allows you to create and maintain up to 10 database branches, enabling you to experiment
with different schemas and data structures without affecting your production data.

- **15 GB of Free Storage**: Xata provides a generous 15 GB of storage space for your data, allowing you to store a
substantial amount of information without worrying about exceeding limits.

- **15 GB of Search Engine Storage**: Xata also offers 15 GB of dedicated storage for its powerful search engine.
This ensures that your data is indexed and easily searchable, making it a breeze to find the information you need.

- **2GB File Attachments**: Xata allows you to attach files of up to 2GB in size to your database records.
This makes it easy to store images, documents, and other binary data alongside your structured data.

- **250 AI Queries Per Month**: Xata provides a generous quota of 250 AI queries per month,
enabling you to leverage artificial intelligence to extract insights from your data.


With all these features available for free, Xata is the perfect choice for startups, individual developers,
and anyone looking for a powerful and cost-effective database solution.

<div style="text-align:center;font-family: Courier">
Join the growing community of developers who have chosen Xata as their trusted database partner.
Unleash your creativity and build amazing things with Xata!
</div>
''',unsafe_allow_html=True)



st.markdown('''
### Unleashing Your Creativity with Xata and Streamlit
<div style="text-align:justify">
The possibilities are endless when you combine the power of Xata and Streamlit, amplified by the st_xatadb_connection
package. Whether you're building a customer relationship management (CRM) system, a data visualization dashboard, or a
real-time monitoring application, this dynamic duo has you covered.

So, let your creativity soar and embark on your next web development project with confidence, knowing that Xata
and Streamlit will be your trusty companions every step of the way.''',unsafe_allow_html=True)
