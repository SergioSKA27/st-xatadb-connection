from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Literal, Optional, Tuple, Union, types


from streamlit.connections import BaseConnection
from xata.client import XataClient
from xata.helpers import to_rfc339
from xata.api_response import ApiResponse
from xata.errors import XataServerError
from pandas import DataFrame
from datetime import datetime, timezone

#from streamlit.runtime.caching import cache_data

__version__ = "0.0.2"


class XataTable:
    def __init__(self,client:XataClient,table_name:str,fixdates:Optional[bool]=False) -> None:
        """
        The `__init__` method initializes a new `XataTable` object.

        :param client: The `XataClient` object that represents the connection to the database.
        :type client: XataClient
        :param table_name: The name of the table that you want to interact with.
        :type table_name: str
        """
        self._fixdates = fixdates
        self._client = client
        self.table_name = table_name
        self.schema = DataFrame(self._client.table().get_schema(self.table_name)['columns'])

    def query(self,full_query:Optional[dict]=None,consistency:Optional[Literal['strong','eventual']]=None,**kwargs) -> ApiResponse:
        """
        The function `query` takes in a full query and consistency level as optional parameters, and returns the result of
        executing the query using the specified consistency level.

        :param full_query: The full_query parameter is a dictionary that represents the query to be executed. It can contain
        various keys and values, but the most common key is 'columns', which specifies the columns to be returned in the query
        result. The value of 'columns' can be a list of column names or '*' to
        :type full_query: Optional[dict]
        :param consistency: The `consistency` parameter is an optional parameter that specifies the consistency level for the
        query. It can have two possible values: "strong" or "eventual" by default it is set to "strong".
        :type consistency: Optional[Literal['strong','eventual']]
        :return: an ApiResponse.
        """
        if consistency is not None and full_query is not None:
            full_query['consistency'] = consistency
        elif consistency is not None and full_query is None:
            full_query = {'consistency':consistency}

        if full_query is None:
            response = self._client.data().query(f'{self.table_name}',**kwargs)
            if not response.is_success():
                raise Exception(response.status_code,response.server_message())
            return response

        response = self._client.data().query(f'{self.table_name}',full_query,**kwargs)
        if not response.is_success():
            raise Exception(response.status_code,response.server_message())
        return self._client.data().query(f'{self.table_name}',full_query,**kwargs)

    def get_record(self,record_id:str,
                db_name: Optional[str]=None,
                branch_name: Optional[str]=None,
                columns: Optional[list]=None) -> ApiResponse:
        """
        The function `get_record` retrieves a record from a table using the provided record ID.

        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record you want
        to retrieve
        :type record_id: str
        :return: an ApiResponse object.
        """
        response = self._client.records().get(f'{self.table_name}',record_id,db_name,branch_name,columns)
        if not response.is_success():
            raise Exception(response.status_code,response.server_message())
        return response


    def insert(self,record:dict,record_id:Optional[str]=None,**kwargs) -> ApiResponse:
        """
        The function inserts a record into a table with an optional record ID.

        :param table_name: The name of the table where the record will be inserted
        :type table_name: str
        :param record: The `record` parameter is a dictionary that represents the data to be inserted into the table. Each
        key-value pair in the dictionary represents a column name and its corresponding value in the table
        :type record: dict
        :param record_id: The `record_id` parameter is an optional parameter that specifies the unique identifier for the
        record. If a `record_id` is provided, the record will be inserted with that specific identifier. If `record_id` is
        not provided, a new unique identifier will be generated for the record
        :type record_id: Optional[str]
        :return: The code is returning an ApiResponse.
        """
        if record_id is not None:
            response = self._client.records().insert_with_id(f'{self.table_name}',record_id,record,**kwargs)
            if not response.is_success():
                raise Exception(response.status_code,response.server_message())
            return response

        response = self._client.records().insert(f'{self.table_name}',record,**kwargs)
        if not response.is_success():
            raise Exception(response.status_code,response.server_message())
        return response

    def replace(self,record_id:str,record:dict,**kwargs) -> ApiResponse:
        """
        The function replaces a record in a table with a new record.

        :param table_name: The name of the table where the record will be replaced
        :type table_name: str
        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record you want
        to replace in the table
        :type record_id: str
        :param record: The `record` parameter is a dictionary that represents the data of the record you want to replace. It
        contains key-value pairs where the keys represent the field names of the record and the values represent the new
        values you want to set for those fields
        :type record: dict
        :return: an ApiResponse.
        """
        return self._client.records().upsert(f'{self.table_name}',record_id,record,**kwargs)

    def update(self,record_id:str,record:dict,**kwargs) -> ApiResponse:
        """
        The function updates a record in a specified table using the provided record ID and record data.

        :param table_name: The name of the table where the record is located
        :type table_name: str
        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record you want
        to update in the specified table
        :type record_id: str
        :param record: The `record` parameter is a dictionary that contains the updated values for the record. Each
        key-value pair in the dictionary represents a field in the record and its updated value
        :type record: dict
        :return: an ApiResponse.
        """
        return self._client.records().update(f'{self.table_name}',record_id,record,**kwargs)

    def delete(self,record_id:str,**kwargs) -> ApiResponse:
        """
        The function deletes a record from a specified table using the provided record ID.

        :param table_name: The name of the table from which you want to delete a record
        :type table_name: str
        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record that you
        want to delete from the specified table
        :type record_id: str
        :return: an ApiResponse.
        """
        return self._client.records().delete(f'{self.table_name}',record_id,**kwargs)

    def search_on_table(self,search_query:dict,**kwargs) -> ApiResponse:
        """
        The function searches for a specific query in a table and returns the results.
        For more information visit: https://xata.io/docs/sdk/search

        :param table_name: The name of the table where you want to perform the search
        :type table_name: str
        :param search_query: The `search_query` parameter is a dictionary that contains the search criteria for the table.
        It can include one or more key-value pairs, where the keys represent the column names in the table and the values
        represent the search values for those columns
        :type search_query: dict
        :return: an ApiResponse.
        """
        return self._client.data().search_table(f'{self.table_name}',search_query,**kwargs)

    def vector_search(self,search_query:dict,**kwargs) -> ApiResponse:
        """
        The function performs a vector search on a specified table using a search query and additional arguments.
        For more information visit: https://xata.io/docs/sdk/vector-search

        :param table_name: The name of the table in which you want to perform the vector search
        :type table_name: str
        :param search_query: The `search_query` parameter is a dictionary that contains the search query for vector search.
        It typically includes the following key-value pairs:
        :type search_query: dict
        :return: an ApiResponse.
        """
        return self._client.data().vector_search(f'{self.table_name}',search_query,**kwargs)

    def aggregate(self,table_name:str,aggregate_query:dict,**kwargs) -> ApiResponse:
        """
        The function aggregates data from a specified table using a given query and returns the result.

        For more information visit: https://xata.io/docs/sdk/aggregate

        :param table_name: The name of the table or collection in the database that you want to perform the aggregation on
        :type table_name: str
        :param aggregate_query: The `aggregate_query` parameter is a dictionary that specifies the aggregation operations to
        be performed on the data in the specified table.
        :type aggregate_query: dict
        :return: an ApiResponse.
        """
        return self._client.data().aggregate(f'{table_name}',aggregate_query,**kwargs)

    def summarize(self,summarize_query:dict,**kwargs) -> ApiResponse:
        """
        The function takes a table name and a summarize query as input and returns the summarized data from the table.

        For more information visit: https://xata.io/docs/sdk/summarize

        :param table_name: The name of the table that you want to summarize
        :type table_name: str
        :param summarize_query: The `summarize_query` parameter is a dictionary that contains the query parameters for the
        summarize operation. It typically includes information such as the columns to group by, the columns to aggregate,
        and any filters to apply to the data
        :type summarize_query: dict
        :return: an ApiResponse.
        """
        return self._client.data().summarize(f'{self.table_name}',summarize_query,**kwargs)

    def _fix_dates(self,payload:dict,time_zone:Optional[timezone]=timezone.utc) -> dict:
        sch = self.schema
        sch = sch[sch['type'].isin(['date','datetime','string'])]

        for col in sch['name']:
            if isinstance(payload[col],datetime):
                payload[col] = to_rfc339(payload[col],time_zone)
            if re.match(r'^\d{4}-\d{2}-\d{2}$',payload[col]):
                date_without_time = datetime.strptime(payload[col], "%Y-%m-%d")
            elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',payload[col]):
                date_without_time = datetime.strptime(payload[col], "%Y-%m-%d %H:%M:%S")

            payload[col] = to_rfc339(date_without_time,time_zone)



class XataConnection(BaseConnection[XataClient]):
    """"
    XataDBConnection is a class that represents a connection to a Xata database.
    It is used to connect to a Xata database and perform various operations on the database.
    attributes:
    - client: The `XataClient` object that represents the connection to the database.
    - table_names: A list of table names that you want to access.

    for more information visit: https://xata-py.readthedocs.io/en/latest/api.html#

    """

    def __init__(self,connection_name:Optional[str]='xata',**kwargs):
        """
        The above function is a constructor that initializes an object with an optional connection name parameter.

        :param connection_name: The connection_name parameter is an optional string that specifies the name of the
        connection. If no value is provided, it defaults to 'xata', defaults to xata
        :type connection_name: Optional[str] (optional)
        """
        super().__init__(connection_name,**kwargs)


    def _connect(self,api_key:Optional[str]=None,
                db_url:Optional[str]=None,
                table_names:Optional[list]=None,
                fixdates:Optional[bool]=False,
                return_metadata:Optional[bool]=True,
                returntype:Optional[Union[Literal['dataframe','dict'],type]]=ApiResponse,
                **kwargs) -> None:
        """
        The `_connect` function establishes a connection to a database using an API key and a database URL.

        Parameters:

            api_key: (Optional) The API key used to authenticate and authorize access to the Xata API. It is a string representing a unique identifier for your account or application.
                If not provided, it will be retrieved from the `XATA_API_KEY` environment variable or the secrets manager.
            db_url: (Optional) The URL of the database that you want to connect to.
                It is a required parameter and must be provided either as an argument to the `_connect` method or through the `XATA_DB_URL` environment variable or the secrets manager.
                If no db_url provided, you'll need to specify the database name and region on each query.
            table_names: (Optional) A list of table names that you want to access.
                If provided, the `_connect` function will create a `XataTable` object for each table name and assign it to a corresponding attribute in the `XataClient` object.

        Raises:

            ConnectionRefusedError: If the API key or the database URL cannot be found.

        Additional Information:

            -The `_connect` function is used internally by the `XataConnection` class to establish a connection to a Xata database.

            -The fixdates parameter is used to specify whether or not to fix the date and time values in the database queries.

            -The return_metadata parameter is used to specify whether or not to return the metadata along with the data in the database responses.

            -The returntype parameter is used to specify the return type of the database responses.

            -The table_names parameter is used to create `XataTable` objects for the specified table names and assign them to corresponding attributes in the `XataClient` object. This allows you to access the tables in the database using the XataTable objects.

            -The kwargs parameter is used to pass additional keyword arguments to the `XataClient` constructor.

        """

        self._fixdates = fixdates
        self._return_metadata = return_metadata
        self._returntype = returntype
        self.client_kwargs = kwargs
        self._table_names = None

        try:
            self._call_client(api_key=api_key,db_url=db_url,**kwargs) # Verify that the connection is working
        except Exception as err:
            raise ConnectionRefusedError("Could not connect to the database. Please check your credentials and try again.") from err

        if table_names is not None and db_url is not None:
            #Now you need to use the database name once and automatically the base url will be set
            #the database url is necessary to get the schema of the tables so if it is not provided, the tables will not be created
            self._table_names = table_names
            for table_name in table_names:
                setattr(self,table_name,XataTable(self._client,table_name))

    def  _call_client(self,api_key:Optional[str]=None,db_url:Optional[str]=None,**kwargs) -> XataClient:
        """
        The `_call_client` function is used to create an instance of the `XataClient` class with the provided API key and
        database URL, or retrieve them from environment variables or secrets manager if not provided.

        :param api_key: The `api_key` parameter is an optional string that represents the API key used for authentication with
        the Xata API. If not provided, the function will attempt to retrieve the API key from the `_secrets` dictionary or the
        secrets manager. If it is still not found, an error will be raised.
        you need to set api_key using the `XATA_API_KEY` environment variable.
         :type api_key: Optional[str]

        :param db_url: The `db_url` parameter is used to specify the URL of the Xata database. If it is not provided, the code
        will try to retrieve it from the secrets manager or the environment variables. If it is still not found, an error will
        be raised. you need to set db_url using the `XATA_DB_URL` environment variable.
        :type db_url: Optional[str]

        :return: an instance of the `XataClient` class.
        """

        if api_key is None:
            if "XATA_API_KEY" in self._secrets:
                api_key = self._secrets["XATA_API_KEY"]
            elif "XATA_API_KEY" in os.environ:
                api_key = os.environ.get("XATA_API_KEY")
            else:
                raise ConnectionRefusedError("No API key found. Please set the XATA_API_KEY environment variable or add it to the secrets manager.")
        else:
            os.environ["XATA_API_KEY"] = api_key

        if db_url is None:
            if "XATA_DB_URL" in self._secrets:
                db_url = self._secrets["XATA_DB_URL"]
            elif "XATA_DB_URL" in os.environ:
                #If the db_url is not provided, it will be neecessary to specify the database  name and the region on each query
                db_url = os.environ.get("XATA_DB_URL")
            elif 'db_name' in kwargs:
                try:
                    db_url  = XataClient(db_name=kwargs['db_name'],api_key=api_key).databases().get_base_url()
                except Exception as err:
                    raise ConnectionRefusedError("No database URL found. Please set the XATA_DB_URL environment variable or add it to the secrets manager.") from err
        else:
            os.environ["XATA_DB_URL"] = db_url

        if db_url is None:
            return XataClient(api_key=api_key,**kwargs)
        else:
            return XataClient(api_key=api_key,db_url=db_url,**kwargs)


    def query(self,table_name:str,
            full_query:Optional[dict]=None,
            consistency:Optional[Literal['strong','eventual']]=None,
            **kwargs) -> ApiResponse:

        """
        This function queries a table in a database using the provided parameters and returns the response.

        :param table_name: The name of the table in the database that you want to query
        :type table_name: str

        :param full_query: The `full_query` parameter is an optional dictionary that contains all the query parameters
        that you want to include in the request. These parameters can be used to customize the query behavior, such as
        setting the consistency level or specifying filters, select especific columns, sort, etc.
        :type full_query: Optional[dict]

        :param consistency: The `consistency` parameter is used to specify the consistency level for the query. It can have
        two possible values: 'strong' or 'eventual'.
        :type consistency: Optional[Literal['strong','eventual']]

        :return: an instance of the `ApiResponse` class.
        """


        client = self._call_client(**self.client_kwargs)


        if consistency is not None:
            if full_query is None:
                full_query = {'consistency':consistency}
            else:
                full_query['consistency'] = consistency

        if full_query is None:
            response = client.data().query(f'{table_name}',**kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code,response.server_message())
        else:
            response = client.data().query(f'{table_name}',full_query,**kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code,response.server_message())

        return response

    def get_record(self,table_name:str,record_id:str,**kwargs) -> ApiResponse:
        """
        The function `get_record` retrieves a record from a specified table using the provided record ID.

        :param table_name: The name of the table from which you want to retrieve the record
        :type table_name: str

        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record you want
        to retrieve from the specified table
        :type record_id: str

        :return: an instance of the `ApiResponse` class.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().get(f'{table_name}',record_id,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def insert(self,table_name:str,record:dict,record_id:Optional[str]=None,**kwargs) -> ApiResponse:
        """
        The function inserts a record into a table with an optional record ID.

        :param table_name: The name of the table where the record will be inserted
        :type table_name: str

        :param record: The `record` parameter is a dictionary that represents the data to be inserted into the table. Each
        key-value pair in the dictionary represents a column name and its corresponding value in the record
        :type record: dict

        :param record_id: The `record_id` parameter is an optional parameter that specifies the unique identifier for the
        record being inserted into the table. If a `record_id` is provided, the record will be inserted with that specific
        identifier. If `record_id` is not provided, the system will generate a unique identifier for the record
        :type record_id: Optional[str]

        :return: an instance of the `ApiResponse` class.
        """

        client = self._call_client(**self.client_kwargs)

        if record_id is not None:
            response = client.records().insert_with_id(f'{table_name}',record_id,record,**kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code,response.server_message())
        else:
            response = client.records().insert(f'{table_name}',record,**kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code,response.server_message())

        return response

    def replace(self,table_name:str,record_id:str,record:dict,**kwargs) -> ApiResponse:
        """
        The function replaces a record in a table with a new record using the provided record ID and record data.

        :param table_name: The name of the table where the record will be replaced
        :type table_name: str

        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record in the
        table
        :type record_id: str

        :param record: The `record` parameter is a dictionary that contains the data to be updated or inserted into the
        table. It should have key-value pairs where the keys represent the column names in the table and the values
        represent the new values for those columns
        :type record: dict

        :return: an instance of the `ApiResponse` class.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().upsert(f'{table_name}',record_id,record,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def update(self,table_name:str,record_id:str,record:dict,**kwargs) -> ApiResponse:
        """
        The function updates a record in a specified table using the provided record ID and record data.

        :param table_name: The name of the table where the record is located
        :type table_name: str

        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record you want
        to update in the specified table
        :type record_id: str

        :param record: The `record` parameter is a dictionary that contains the updated data for the record. It should have
        key-value pairs where the keys represent the field names in the table and the values represent the updated values
        for those fields
        :type record: dict

        :return: an instance of the `ApiResponse` class.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().update(f'{table_name}',record_id,record,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def delete(self,table_name:str,record_id:str,**kwargs) -> ApiResponse:
        """
        The function deletes a record from a specified table using the provided record ID.

        :param table_name: The name of the table from which you want to delete a record
        :type table_name: str

        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record that you
        want to delete from the specified table
        :type record_id: str

        :return: an instance of the `ApiResponse` class.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().delete(f'{table_name}',record_id,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def search(self,search_query:dict,**kwargs) -> ApiResponse:
        """
        The function searches for a specific query in a branch and returns the results.

        :param search_query: A dictionary containing the search query parameters
        :type search_query: dict

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().search_branch(search_query,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def search_on_table(self,table_name:str,search_query:dict,**kwargs) -> ApiResponse:
        """
        The function searches for data in a specified table using a search query and returns the response.

        :param table_name: The name of the table you want to search in
        :type table_name: str

        :param search_query: The `search_query` parameter is a dictionary that contains the search criteria for the table.
        It can include one or more key-value pairs, where the key represents the column name and the value represents the
        search value
        :type search_query: dict

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().search_table(f'{table_name}',search_query,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def vector_search(self,table_name:str,search_query:dict,**kwargs) -> ApiResponse:
        """
        The function performs a vector search on a specified table using a search query and returns the response.

        :param table_name: The name of the table in which you want to perform the vector search
        :type table_name: str

        :param search_query: The `search_query` parameter is a dictionary that contains the search query for vector search.
        It typically includes the vector field name and the vector value to search for. The specific structure of the
        dictionary may depend on the API or library you are using for vector search
        :type search_query: dict

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().vector_search(f'{table_name}',search_query,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def aggregate(self,table_name:str,aggregate_query:dict,**kwargs) -> ApiResponse:
        """
        The function aggregates data from a specified table using a given query and returns the response.

        :param table_name: The name of the table in the database that you want to perform the aggregate query on
        :type table_name: str

        :param aggregate_query: The `aggregate_query` parameter is a dictionary that contains the query for aggregation. It
        typically includes the fields to group by and the aggregation functions to apply on those fields.
        :type aggregate_query: dict

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().aggregate(f'{table_name}',aggregate_query,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def summarize(self,table_name:str,summarize_query:dict,**kwargs) -> ApiResponse:
        """
        The function takes a table name and a summarize query to summarize the data in the
        table, and returns the response.

        :param table_name: The name of the table that you want to summarize
        :type table_name: str

        :param summarize_query: The `summarize_query` parameter is a dictionary that contains the query parameters for the
        summarize operation. It specifies how the data should be summarized, such as the columns to group by, the columns to
        aggregate, and any filters to apply
        :type summarize_query: dict

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().summarize(f'{table_name}',summarize_query,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())
        return response

    def transaction(self,payload:dict,**kwargs) -> ApiResponse:
        """
        The function performs a transaction using a client and returns the response, raising an exception if the response is
        not successful.

        :param payload: The `payload` parameter is a dictionary that contains the data needed for the transaction. It is
        passed to the `transaction` method of the `client.records()` object
        :type payload: dict

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().transaction(payload,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())
        return response

    def sql_query(self,query:str,**kwargs) -> ApiResponse:
        """
        The function executes an SQL query using a client and returns the response.

        :param query: The `query` parameter is a string that represents the SQL query you want to execute. It can be any
        valid SQL statement, such as SELECT, INSERT, UPDATE, DELETE, etc
        :type query: str

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.sql().query(query,**kwargs)
        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def askai(self,reference_table:str,question:str, rules: Optional[list]=[], options: Optional[dict]={},**kwargs)->ApiResponse:
        """
        The function `askai` takes in a reference table, a question, optional rules and options, and returns an API
        response.

        :param reference_table: The reference_table parameter is a string that represents the table or dataset that you want
        to ask the question to. It is used to specify the context or domain in which the question is being asked
        :type reference_table: str

        :param question: The "question" parameter is a string that represents the question you want to ask the AI
        :type question: str

        :param rules: The `rules` parameter is an optional list that allows you to specify additional rules or constraints
        for the question being asked. These rules can be used to filter or manipulate the data before returning the response
        :type rules: Optional[list]

        :param options: The "options" parameter is a dictionary that allows you to provide additional options for the ask
        query. These options can include things like the number of results to return, the language to use for the query, and
        any additional parameters specific to the ask query
        :type options: Optional[dict]

        :return: an ApiResponse object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().ask(reference_table,question,rules=rules,options=options,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def askai_follow_up(self,reference_table:str,question:str,chatsessionid: str,**kwargs)->ApiResponse:
        """
        The function `askai_follow_up` sends a follow-up question to an AI model using a reference table and a chat session
        ID.

        :param reference_table: The reference_table parameter is a string that represents the name or identifier of the
        table or database where the reference data is stored. This table contains the information that the AI model uses to
        generate responses
        :type reference_table: str

        :param question: The "question" parameter is a string that represents the follow-up question that you want to ask
        the AI
        :type question: str

        :param chatsessionid: The `chatsessionid` parameter is a unique identifier for a chat session. It is used to track
        and identify a specific chat session in the system
        :type chatsessionid: str

        :return: The function `askai_follow_up` returns an `ApiResponse` object.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().ask_follow_up(reference_table,chatsessionid,question,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response


    def __call__(self) -> XataClient:
        """
        The function returns the XataClient object.
        :return: The method is returning an instance of the XataClient class.
        """
        return self._call_client(**self.client_kwargs)
