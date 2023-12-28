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
from datetime import datetime, timezone


#from streamlit.runtime.caching import cache_data

__version__ = "0.1.0"





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

        Raises:

            ConnectionRefusedError: If the API key or the database URL cannot be found.

        Additional Information:

            -The `_connect` function is used internally by the `XataConnection` class to establish a connection to a Xata database.

            -The fixdates parameter is used to specify whether or not to fix the date and time values in the database queries.

            -The return_metadata parameter is used to specify whether or not to return the metadata along with the data in the database responses.

            -The returntype parameter is used to specify the return type of the database responses.

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

    def bulk_insert(self, table_name: str, records: list, **kwargs) -> ApiResponse:
        """
        Inserts multiple records into the specified table.

        Args:
            table_name (str): The name of the table to insert records into.
            records (list): A list of records to be inserted.
            **kwargs: Additional keyword arguments to be passed to the underlying API.

        Returns:
            ApiResponse: The response from the API.

        Raises:
            XataServerError: If the API response indicates an error.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().bulk_insert(f'{table_name}', {'records': records}, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def upload_file(self,table_name:str,record_id:str,column_name:str,file_content:Union[str,bytes],**kwargs) -> ApiResponse:
        """
        Uploads a file to the specified table, record, and column in the XataDB database.

        Args:
            table_name (str): The name of the table where the file will be uploaded.
            record_id (str): The ID of the record where the file will be uploaded.
            column_name (str): The name of the column where the file will be uploaded.
            file_content (Union[str,bytes]): The content of the file to be uploaded.
            **kwargs: Additional keyword arguments to be passed to the XataDB API.

        Returns:
            ApiResponse: The response from the XataDB API.

        Raises:
            XataServerError: If the API response is not successful.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.files().put(f'{table_name}',record_id,column_name,file_content,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def append_file_to_array(self,table_name:str,record_id:str,column_name:str,file_id: str,file_content:Union[str,bytes],**kwargs) -> ApiResponse:
        """
        Appends a file to a specific column in a record of a table.

        Args:
            table_name (str): The name of the table.
            record_id (str): The ID of the record.
            column_name (str): The name of the column.
            file_id (str): The ID of the file to be appended.
            file_content (Union[str, bytes]): The content of the file to be appended.
            **kwargs: Additional keyword arguments to be passed to the underlying API.

        Returns:
                ApiResponse: The response from the API.

        Raises:
            XataServerError: If the API response is not successful.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.files().put_item(f'{table_name}',record_id,column_name,file_id,file_content,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def get_file(self,table_name:str,record_id:str,column_name:str,**kwargs) -> ApiResponse:

        client = self._call_client(**self.client_kwargs)
        response = client.files().get(f'{table_name}',record_id,column_name,**kwargs)
        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def get_file_from_array(self,table_name:str,record_id:str,column_name:str,file_id:str,**kwargs) -> ApiResponse:

        client = self._call_client(**self.client_kwargs)
        response = client.files().get_item(f'{table_name}',record_id,column_name,file_id,**kwargs)
        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def delete_file(self,table_name:str,record_id:str,column_name:str,**kwargs) -> ApiResponse:

        client = self._call_client(**self.client_kwargs)
        response = client.files().delete(f'{table_name}',record_id,column_name,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def delete_file_from_array(self,table_name:str,record_id:str,column_name:str,file_id:str,**kwargs) -> ApiResponse:

        client = self._call_client(**self.client_kwargs)
        response = client.files().delete_item(f'{table_name}',record_id,column_name,file_id,**kwargs)
        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def image_transform(self, image_url: str, transformations: dict, **kwargs) -> bytes:
        """
        Transforms an image using the specified transformations.

        Args:
            image_url (str): The URL of the image to transform.
            transformations (dict): A dictionary containing the transformations to apply to the image.
            **kwargs: Additional keyword arguments.

            Returns:
                bytes: The transformed image data.

        """
        client = self._call_client(**self.client_kwargs)
        response = client.files().transform(image_url, transformations)

        return response

    def _fix_dates(self,payload:dict,time_zone:Optional[timezone]=timezone.utc,table_name:str='') -> dict:
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

    def __call__(self) -> XataClient:
        """
        The function returns the XataClient object.
        :return: The method is returning an instance of the XataClient class.
        """
        return self._call_client(**self.client_kwargs)
