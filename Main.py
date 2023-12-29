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

st.markdown('''
<h1 style="font-family:Courier; text-align:center;">Xata and Streamlit </h1>
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
### Xata: The Flexible and Scalable Data Storage Solution

<div style="text-align:justify">
Imagine a database that can effortlessly adapt to your evolving needs, without the constraints of a rigid schema.
Xata.io brings this vision to life, allowing you to add, remove, and modify columns on the fly. This flexibility makes
it an ideal choice for applications that require frequent changes or those that deal with diverse data types.
</div>

<div style="text-align:justify">
Furthermore, Xata's scalability ensures that your application can handle even the most demanding workloads.
Its cloud-based architecture can seamlessly scale up or down as your data grows or shrinks, ensuring optimal performance at all times.
<br>
</div>

### The st_xatadb_connection Package: Simplifying the Integration Process

<div style="text-align:justify">
To further enhance the synergy between Xata.io and Streamlit, I've created the st_xatadb_connection package.
This package provides a seamless bridge between the two tools, making it incredibly easy to connect your Streamlit
application to your Xata database.
</div>

<div style="text-align:justify">
With just a few lines of code, you can perform CRUD operations (create, retrieve, update, delete) on your Xata.io data,
generate reports and visualizations, and even interact with your data in real time.
The st_xatadb_connection package takes care of all the heavy lifting, allowing you to focus on building your application's functionality.
</div>

### Unleashing Your Creativity with Xata and Streamlit
<div style="text-align:justify">
The possibilities are endless when you combine the power of Xata and Streamlit, amplified by the st_xatadb_connection
package. Whether you're building a customer relationship management (CRM) system, a data visualization dashboard, or a
real-time monitoring application, this dynamic duo has you covered.

So, let your creativity soar and embark on your next web development project with confidence, knowing that Xata
and Streamlit will be your trusty companions every step of the way.
</div>
''',unsafe_allow_html=True)


st.markdown('''

## What is Xata?

### Serverless data platform

<div style="text-align:justify">
Xata is a new type of Cloud service: it combines multiple types of stores (relational database, search engine, analytics engine)
into a single service. The combined functionality is exposed over a single consistent API.
It is also vertically integrated: an advanced admin UI and high-level SDKs come into the package.
</div>

:blue[They call this type of service a Serverless Data Platform.]''',unsafe_allow_html=True)

st.image('https://xata.io/_next/image?url=%2Fmdx%2Fdocs%2F030-Concepts%2Fserverless-data-platform.png&w=1920&q=75',width=600)

st.markdown('''
This type of service has several benefits:

- it simplifies building applications, thanks to its vertical integration and serverless capabilities

- it offers more data-related functionality than a typical database, for example, free-text-search with relevancy controls,
and more advanced analytics

- it grows with you, in both scale and required functionality because it is based on proven data technologies,
each best-in-class for the problems they solve.

<div style="text-align:justify">
Currently, the Xata service uses PostgreSQL as the data store, Elasticsearch as the search/analytics engine,
and Kafka for storing the logical replication events. It also offers support for edge-caching.
</div>

<div style="text-align:justify">
In the future, they plan to add built-in support for centralized caching (for example, Redis), block storage,
better support for sharding between DBs, automatic global distribution, and so on.
</div>

<div style="text-align:justify">
This type of data architecture, where a relational DB is used as the primary store and the data in it is replicated
in other more specialized stores (search engine, data warehousing, business intelligence systems, etc.)
is very popular among companies over a certain size. However, it requires significant amounts of glue code,
operational overhead, monitoring, and expertise. Not anymore, Xata packages it all in a single opinionated service.
</div>''',unsafe_allow_html=True)

st.markdown('''

### How is it Different from Backend-as-a-Service (BaaS)?

<div style="text-align:justify">
Backend-as-a-Service type of service is similar because it also offers the functionality of a serverless DB with more
services on top of it. Firebase popularized the model already more than 10 years ago, and more recently players like
Supabase, AppWrite, NHost, and others are offering open-source BaaS.
</div>

<div style="text-align:justify">
The difference is the type of functionality that is added on top: BaaS typically try to add all the functionality
that an app needs: hosting, authentication, push notifications, block storage.
</div>

<div style="text-align:justify">
A Serverless Data Platform, on the other hand, is focused on adding data-related functionality, like free-text-search,
advanced aggregations, caching, block storage, and so on. It doesn't provide things like hosting, authentication,
or push notifications, but it integrates nicely with platforms like Vercel, Netlify, or Cloudflare pages,
as well as other authentication providers.
</div>''',unsafe_allow_html=True)


st.image('https://xata.io/_next/image?url=%2Fmdx%2Fdocs%2F030-Concepts%2Fbaas-vs-sdp.png&w=1920&q=75',width=600)


st.markdown('''
<div style="text-align:justify">
Because BaaS typically provide built-in authentication and row-level-security rules, it is possible to write strictly
the client side of the application, and write no backend code at all (hence the name).
This can be great for small projects because they get everything from a single provider, but as the projects grow in
requirements and scale, some serious challenges appear:
</div>

- security rules become more complex and they are better implemented and maintained in code

- each sub-service in the BaaS (hosting, authentication) is relatively shallow compared to dedicated providers

- there is significant functionality overlap between what the BaaS offers and what modern full-stack web frameworks offer

This causes a lot of product teams to move away from BaaS when they grow past a certain scale.
''',unsafe_allow_html=True)

with Profiler():
    st.write(xata.query('Alumno'))
