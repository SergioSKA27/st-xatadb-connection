import streamlit as st


st.markdown('<h1> Xata Data Base  <img src="https://xata.io/icon.svg?9d7a66ec4c0ad6b1" width="50" height="50" align-items="center"/></h1>', unsafe_allow_html=True)
st.markdown('''

## What is Xata?

### Serverless data platform

<div style="text-align:justify">
Xata is a new type of Cloud service: it combines multiple types of stores (relational database, search engine, analytics engine)
into a single service. The combined functionality is exposed over a single consistent API.
It is also vertically integrated: an advanced admin UI and high-level SDKs come into the package.
</div>

:blue[They call this type of service a Serverless Data Platform.]''',unsafe_allow_html=True)


st.markdown('''
<div style="align-items: center;justify-content: center;display: flex;">
<img src="https://xata.io/_next/image?url=%2Fmdx%2Fdocs%2F030-Concepts%2Fserverless-data-platform.png&w=1920&q=75" width="600" style="background-color: grey; border-radius: 10px; padding: 10px; margin: 10px;"/>
</div>
''',unsafe_allow_html=True)

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



st.markdown('''
<div style="align-items: center;justify-content: center;display: flex;">
<img src="https://xata.io/_next/image?url=%2Fmdx%2Fdocs%2F030-Concepts%2Fbaas-vs-sdp.png&w=1920&q=75" width="600" style="background-color: grey; border-radius: 10px; padding: 10px; margin: 10px;"/>
</div>
''',unsafe_allow_html=True)



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
