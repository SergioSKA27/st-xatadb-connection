from __future__ import annotations

import os
import uuid
from typing import Literal, Optional, Union,List,Dict,Tuple


from streamlit.connections import BaseConnection
from xata.client import XataClient
from xata.helpers import BulkProcessor,Transaction
from xata.api_response import ApiResponse
from xata.api_request import ApiRequest
from xata.errors import XataServerError


#from streamlit.runtime.caching import cache_data
#By: Sergio Demis Lopez Martinez


__version__ = "1.0.3"

__all__ = ["XataConnection"]



class XataConnection(BaseConnection[XataClient]):

    """"

    XataDBConnection Module Documentation
    =====================================

    The `XataConnection` class represents a connection to a Xata database. It provides methods to connect to the database and perform various operations.
    github repo: https://github.com/SergioSKA27/st-xatadb-connection

    Usage:
    ------
    ```python
    from st_xatadb_connection import XataConnection

    # Create a connection to the Xata database
    xata_connection = st.connection('xata', type=XataConnection)

    # Example: Execute a query
    result = xata_connection.query('example_table')
    print(result)
    ```

    Xata Docs:
    For more information about Xata, visit

        - Xata Examples: https://xata.io/docs

        - API documentation: https://xata-py.readthedocs.io/en/latest/api.html#
    """

    def __init__(self,connection_name:Optional[str]='xata',**kwargs):
        """
        The above function is a constructor that initializes an object with an optional connection name parameter.

        :param connection_name: The connection_name parameter is an optional string that specifies the name of the
        connection. If no value is provided, it defaults to 'xata', defaults to xata
        :type connection_name: Optional[str] (optional)
        """
        super().__init__(connection_name,**kwargs)

    def _call_client(self,api_key:Optional[str]=None,db_url:Optional[str]=None,**kwargs) -> XataClient:
            """
            This method is used to create an instance of the XataClient class.

            Parameters:
            - api_key (Optional[str]): The API key to authenticate the client. If not provided, it will be retrieved from the secrets manager or environment variables.
            - db_url (Optional[str]): The URL of the database. If not provided, it will be retrieved from the secrets manager or environment variables.
            - kwargs: Additional keyword arguments to be passed to the XataClient constructor.

            Returns:
            - XataClient: An instance of the XataClient class.

            Raises:
            - ConnectionRefusedError: If no API key is found in the secrets manager or environment variables.
            """
            if api_key is None:
                if "XATA_API_KEY" in self._secrets:
                    api_key = self._secrets["XATA_API_KEY"]
                elif "XATA_API_KEY" in os.environ:
                    api_key = os.environ.get("XATA_API_KEY")
                elif 'XATA_API_KEY' in self.__secrets and self.__secrets['XATA_API_KEY'] is not None:
                    api_key = self.__secrets['XATA_API_KEY']
                else:
                    raise ConnectionRefusedError("No API key found. Please set the XATA_API_KEY environment variable or add it to the secrets manager.")

            #If the db_url is not provided, it will be neecessary to specify the database  name and the region  when calling the client
            if db_url is None:
                if "XATA_DB_URL" in self._secrets:
                    db_url = self._secrets["XATA_DB_URL"]
                elif "XATA_DB_URL" in os.environ:
                    db_url = os.environ.get("XATA_DB_URL")
                elif "XATA_DB_URL" in self.__secrets and self.__secrets["XATA_DB_URL"] is not None:
                    db_url = self.__secrets["XATA_DB_URL"]

            if db_url is None:
                return XataClient(api_key=api_key,**kwargs)
            else:
                return XataClient(api_key=api_key,db_url=db_url,**kwargs)

    def _connect(self,api_key:Optional[str]=None,db_url:Optional[str]=None,**kwargs) -> None:
        """
        Connects to the Xata database using the provided API key and database URL.

         Args:
            api_key (str, optional): The API key for accessing the Xata database. Defaults to None.
            db_url (str, optional): The URL of the Xata database. Defaults to None.
            **kwargs: Additional keyword arguments to be passed to the XataClient constructor.
            """
        self.client_kwargs = kwargs
        self.__secrets = {'XATA_API_KEY': api_key, 'XATA_DB_URL': db_url} # Not recommended  to pass the api_key and db_url as kwargs

        self._call_client(api_key=api_key,db_url=db_url,**kwargs) # Verify that the connection is working

    def query(self, table_name: str, full_query: Optional[dict] = None, **kwargs) -> ApiResponse:
        """
        Executes a query on the specified table.

        Args:
            table_name (str): The name of the table to query.
            full_query (dict, optional): A dictionary containing additional query parameters. Defaults to None.
            **kwargs: Additional keyword arguments to be passed to the query.

        Returns:
            ApiResponse: The response from the query.

        Raises:
            XataServerError: If the query response is not successful.

        For more information visit: https://xata.io/docs/sdk/get
        """

        client = self._call_client(**self.client_kwargs)
        response = client.data().query(f'{table_name}', full_query, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def get(self, table_name: str, record_id: str, columns: Optional[list] = None, **kwargs) -> ApiResponse:
        """
            Retrieves a record from the specified table.

            Args:
                table_name (str): The name of the table.
                record_id (str): The ID of the record to retrieve.
                columns (Optional[list]): A list of column names to include in the response. Defaults to None.
                **kwargs: Additional keyword arguments to pass to the API.

            Returns:
                ApiResponse: The response from the API.

            Raises:
                XataServerError: If the API response is not successful.

            For more information visit: https://xata.io/docs/sdk/get
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().get(f'{table_name}', record_id, columns=columns, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def insert(self, table_name: str, record: dict, record_id: Optional[str] = None,
               create_only: Optional[bool] = None, if_version: Optional[int] = None,
               columns: Optional[list] = None, **kwargs) -> ApiResponse:
        """
        Inserts a record into the specified table.

        Args:
            table_name (str): The name of the table.
            record (dict): The record to be inserted.
            record_id (str, optional): The ID of the record. If not provided, a new UUID will be generated.
            create_only (bool, optional): If set to True, the record will only be created if it doesn't already exist.
            if_version (int, optional): The version of the record to check before inserting. If provided, the record will only be inserted if the current version matches the specified version.
            columns (list, optional): A list of column names to include in the insert operation.
            **kwargs: Additional keyword arguments to be passed to the insert operation.

        Returns:
            ApiResponse: The response from the insert operation.

        Raises:
            XataServerError: If the insert operation fails.

        For more information visit: https://xata.io/docs/sdk/insert
        """
        client = self._call_client(**self.client_kwargs)

        if record_id is not None or (create_only is not None or if_version is not None or columns is not None):
            if record_id is None:
                record_id = str(uuid.uuid4())

            response = client.records().insert_with_id(f'{table_name}', record_id, record,
                                                       create_only=create_only, if_version=if_version,
                                                       columns=columns, **kwargs)
        else:
            response = client.records().insert(f'{table_name}', record, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def upsert(self, table_name: str, record_id: str, record: dict,
                    if_version: Optional[int] = None, columns: Optional[list] = None,
                    **kwargs) -> ApiResponse:
        """
            Upserts a record into the specified table.

            Args:
                table_name (str): The name of the table.
                record_id (str): The ID of the record.
                record (dict): The record data to upsert.
                if_version (Optional[int], optional): The version of the record to check before upserting. Defaults to None.
                columns (Optional[list], optional): The list of columns to include in the upsert. Defaults to None.
                **kwargs: Additional keyword arguments to pass to the upsert method.

            Returns:
                ApiResponse: The response from the upsert operation.

            Raises:
                XataServerError: If the upsert operation is not successful.
            """

        client = self._call_client(**self.client_kwargs)
        response = client.records().upsert(f'{table_name}', record_id, record, columns=columns, if_version=if_version, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def update(self,table_name:str,record_id:str,
                    record:dict,if_version:Optional[int]=None,columns:Optional[list]=None,
                    **kwargs) -> ApiResponse:
            """
            Updates a record in the specified table.

            Args:
                table_name (str): The name of the table.
                record_id (str): The ID of the record to update.
                record (dict): The updated record data.
                if_version (Optional[int]): The version of the record to update (optional).
                columns (Optional[list]): The names of the columns to update (optional).
                **kwargs: Additional keyword arguments to pass to the update method.

            Returns:
                ApiResponse: The response from the update operation.

            Raises:
                XataServerError: If the update operation is not successful.
            """

            client = self._call_client(**self.client_kwargs)
            response = client.records().update(f'{table_name}',record_id,record,if_version=if_version,columns=columns,**kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code,response.server_message())

            return response

    def delete(self, table_name: str, record_id: str, columns: Optional[list] = None, **kwargs) -> ApiResponse:
            """
            Deletes a record from the specified table.

            Args:
                table_name (str): The name of the table.
                record_id (str): The ID of the record to delete.
                columns (Optional[list]): A list of column names to delete. Defaults to None.
                **kwargs: Additional keyword arguments to pass to the delete method.

            Returns:
                ApiResponse: The response from the delete operation.

            Raises:
                XataServerError: If the delete operation is not successful.
            """

            client = self._call_client(**self.client_kwargs)
            response = client.records().delete(f'{table_name}', record_id, columns=columns, **kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code, response.server_message())

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

    def transaction(self,payload:Union[List[Dict],Dict],**kwargs) -> ApiResponse:
        """
        The function performs a transaction using a client and returns the response, raising an exception if the response is
        not successful.

        :param payload: The `payload` parameter is a dictionary that contains the data needed for the transaction. It is
        passed to the `transaction` method of the `client.records()` object
        :type payload: dict

        :return: an ApiResponse object.
        """
        if "operations" not in payload:
            payl = {"operations":payload}
        else:
            payl = payload

        client = self._call_client(**self.client_kwargs)
        response = client.records().transaction(payl,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())
        return response

    def sql_query(self, query: str, params: Optional[list] = None, consistency: Optional[Literal['strong', 'eventual']] = 'strong', **kwargs) -> ApiResponse:
        """
            Executes a SQL query on the Xata database.

            Args:
                query (str): The SQL query to execute.
                params (Optional[list]): Optional parameters to be used in the query.
                consistency (Optional[Literal['strong', 'eventual']]): The consistency level for the query. Defaults to 'strong'.
                **kwargs: Additional keyword arguments to be passed to the query.

            Returns:
                ApiResponse: The response from the Xata database.

            Raises:
                XataServerError: If the query execution is not successful.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.sql().query(query, params, consistency=consistency, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def askai(self, reference_table: str, question: str,
              rules: Optional[list] = None, options: Optional[dict] = None,
              streaming_results: Optional[bool] = False, **kwargs) -> ApiResponse:
        """
        Sends a question to the Xata AI service and retrieves the response.

        Args:
            reference_table (str): The reference table for the question.
            question (str): The question to ask.
            rules (Optional[list], optional): List of rules to apply. Defaults to None.
            options (Optional[dict], optional): Additional options for the question. Defaults to None.
            streaming_results (Optional[bool], optional): Whether to stream the results. Defaults to False.
            **kwargs: Additional keyword arguments to pass to the Xata AI service.

        Returns:
            ApiResponse: The response from the Xata AI service.

        Raises:
            XataServerError: If the response from the Xata AI service is not successful.
        """
        client = self._call_client(**self.client_kwargs)
        if rules is None:
            rules = []

        if options is None:
            options = {}

        response = client.data().ask(reference_table, question, rules=rules, options=options,
                                    streaming_results=streaming_results, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def askai_follow_up(self, reference_table: str, question: str,
                            chatsessionid: str, streaming_results: Optional[bool] = False,
                            **kwargs) -> ApiResponse:
        """
            Sends a follow-up question to the AI model and retrieves the response.

            Args:
                reference_table (str): The reference table for the AI model.
                question (str): The question to ask the AI model.
                chatsessionid (str): The ID of the chat session.
                streaming_results (bool, optional): Whether to stream the results or not. Defaults to False.
                **kwargs: Additional keyword arguments to pass to the API.

            Returns:
                ApiResponse: The response from the API.

            Raises:
                XataServerError: If the API response is not successful.
            """

        client = self._call_client(**self.client_kwargs)
        response = client.data().ask_follow_up(reference_table, chatsessionid, question,
                                                   streaming_results=streaming_results, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

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

        For more information visit: https://xata.io/docs/sdk/insert
        """

        client = self._call_client(**self.client_kwargs)
        response = client.records().bulk_insert(f'{table_name}', {'records': records}, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def upload_file(self,table_name:str,record_id:str,
                column_name:str,file_content: bytes,
                content_type:Optional[str]='application/octet-stream',**kwargs) -> ApiResponse:
        """
        Uploads a file to the specified table, record, and column in the XataDB database.

        Args:
            table_name (str): The name of the table where the file will be uploaded.
            record_id (str): The ID of the record where the file will be uploaded.
            column_name (str): The name of the column where the file will be uploaded.
            file_content (Union[str,bytes]): The content of the file to be uploaded.
            content_type (Optional[str], optional): The content type of the file. Defaults to 'application/octet-stream'.
            **kwargs: Additional keyword arguments to be passed to the XataDB API.

        Returns:
            ApiResponse: The response from the XataDB API.

        Raises:
            XataServerError: If the API response is not successful.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.files().put(f'{table_name}',record_id,column_name,file_content,content_type,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def append_file_to_array(self,table_name:str,record_id:str,column_name:str,
                            file_id: str,file_content:bytes,
                            content_type:Optional[str]='application/octet-stream',**kwargs) -> ApiResponse:
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
        response = client.files().put_item(f'{table_name}',record_id,column_name,file_id,file_content,content_type,**kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response

    def get_file(self, table_name: str, record_id: str, column_name: str, **kwargs) -> ApiResponse:
        """
        Retrieves a file from the specified table, record, and column.

        Args:
            table_name (str): The name of the table.
            record_id (str): The ID of the record.
            column_name (str): The name of the column.
            **kwargs: Additional keyword arguments to be passed to the API.

        Returns:
            ApiResponse: The response from the API.

        Raises:
            XataServerError: If the API response is not successful.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.files().get(f'{table_name}', record_id, column_name, **kwargs)
        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def get_file_from_array(self, table_name: str, record_id: str, column_name: str, file_id: str, **kwargs) -> ApiResponse:
        """
        Retrieves file content from an array by file ID

        Args:
            table_name (str): The name of the table.
            record_id (str): The ID of the record.
            column_name (str): The name of the array column.
            file_id (str): The ID of the file within the array.
            **kwargs: Additional keyword arguments to be passed to the API.

        Returns:
            ApiResponse: The response from the API.

        Raises:
            XataServerError: If the API response is not successful.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.files().get_item(f'{table_name}', record_id, column_name, file_id, **kwargs)
        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def delete_file(self, table_name: str, record_id: str, column_name: str, **kwargs) -> ApiResponse:
            """
            Deletes a file from a specific table and record in the Xata database.

            Args:
                table_name (str): The name of the table.
                record_id (str): The ID of the record.
                column_name (str): The name of the column where the file is stored.
                **kwargs: Additional keyword arguments to be passed to the API.

            Returns:
                ApiResponse: The response from the API.

            Raises:
                XataServerError: If the API response is not successful.
            """

            client = self._call_client(**self.client_kwargs)
            response = client.files().delete(f'{table_name}', record_id, column_name, **kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code, response.server_message())

            return response

    def delete_file_from_array(self,table_name:str,record_id:str,column_name:str,file_id:str,**kwargs) -> ApiResponse:
            """
            Deletes a file from an array field in a record.

            Args:
                table_name (str): The name of the table.
                record_id (str): The ID of the record.
                column_name (str): The name of the array field.
                file_id (str): The ID of the file to delete.
                **kwargs: Additional keyword arguments to pass to the API.

            Returns:
                ApiResponse: The API response.

            Raises:
                XataServerError: If the API response is not successful.
            """

            client = self._call_client(**self.client_kwargs)
            response = client.files().delete_item(f'{table_name}',record_id,column_name,file_id,**kwargs)
            if not response.is_success():
                raise XataServerError(response.status_code,response.server_message())

            return response

    def image_transform(self, image_url: str, transformations: dict) -> bytes:
        """
        Transforms an image using the specified transformations.

        Args:
            image_url (str): The URL of the image to transform.
            transformations (dict): A dictionary containing the transformations to apply to the image.

            Returns:
                bytes: The transformed image data.

        """
        client = self._call_client(**self.client_kwargs)
        response = client.files().transform(image_url, transformations)

        return response

    def next_page(self, table_name: str, response_prev: ApiResponse,
                    pagesize: Optional[int] = 20,
                    offset: Optional[int] = None,
                    limit: Optional[int] = None,
                    consistency: Optional[Literal['strong', 'eventual']] = None,
                    **kwargs) -> Union[ApiResponse, None]:
        """
        Retrieves the next page of results from the specified table.

        Args:
            table_name (str): The name of the table to query.
            response_prev (ApiResponse): The previous API response containing the cursor for the next page.
            pagesize (int, optional): The number of results to retrieve per page. Defaults to 20.
            offset (int, optional): The offset to start retrieving results from. Defaults to None.
            consistency (str, optional): The consistency level to use for the query. Defaults to None.
            **kwargs: Additional keyword arguments to pass to the query.

        Returns:
            Union[ApiResponse, None]: The next page of results as an ApiResponse object, or None if there are no more results.
        """
        client = self._call_client(**self.client_kwargs)

        _next = {'size': pagesize, 'after': response_prev.get_cursor()}
        if offset is not None:
            _next['offset'] = offset

        if limit is not None:
            _next['limit'] = limit

        if consistency is not None:
            _next['consistency'] = consistency

        if response_prev.has_more_results():
            nextpage = client.data().query(f'{table_name}', {'page': _next}, **kwargs)

            if not nextpage.is_success():
                raise XataServerError(nextpage.status_code, nextpage.server_message())
        else:
            nextpage = None

        return nextpage

    def prev_page(self, table_name: str, response_after: ApiResponse,
                        pagesize: Optional[int] = 20,
                        offset: Optional[int] = None,
                        limit: Optional[int] = None,
                        consistency: Optional[Literal['strong', 'eventual']] = None,
                        **kwargs) -> Union[ApiResponse, None]:
        """
            Retrieves the previous page of results from the specified table.

            Args:
                table_name (str): The name of the table.
                response_after (ApiResponse): The response object representing the current page.
                pagesize (Optional[int], optional): The number of items to retrieve per page. Defaults to 20.
                offset (Optional[int], optional): The offset from the beginning of the result set. Defaults to None.
                limit (Optional[int], optional): The maximum number of items to retrieve. Defaults to None.
                consistency (Optional[Literal['strong', 'eventual']], optional): The consistency level for the query.
                    Defaults to None.
                **kwargs: Additional keyword arguments to be passed to the query.

            Returns:
                Union[ApiResponse, None]: The response object representing the previous page of results,
                or None if there are no more results.
        """

        client = self._call_client(**self.client_kwargs)

        _next = {'size': pagesize, 'before': response_after.get_cursor()}

        if offset is not None:
            _next['offset'] = offset

        if limit is not None:
            _next['limit'] = limit

        if consistency is not None:
            _next['consistency'] = consistency

        if response_after.has_more_results():
            nextpage = client.data().query(f'{table_name}', {'page': _next}, **kwargs)

            if not nextpage.is_success():
                raise XataServerError(nextpage.status_code, nextpage.server_message())
        else:
            nextpage = None

        return nextpage

    def get_schema(self , table_name: str, **kwargs) -> ApiResponse:
            """
            Retrieves the schema of a table from the Xata database.

            Args:
                table_name (str): The name of the table.
                **kwargs: Additional keyword arguments to be passed to the Xata client.

            Returns:
                ApiResponse: The response from the Xata client.

            Raises:
                XataServerError: If the response from the Xata client is not successful.
            """
            client = self._call_client(**self.client_kwargs)
            response = client.table().get_schema(table_name, **kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code, response.server_message())

            return response

    def create_table(self, table_name: str, schema: dict, **kwargs) -> Tuple[ApiResponse, ApiResponse]:
            """
            Creates a table in the Xata database with the given table name and schema.

            Args:
                table_name (str): The name of the table to be created.
                schema (dict): The schema of the table, represented as a dictionary.
                **kwargs: Additional keyword arguments to be passed to the Xata client.

            Returns:
                Tuple[ApiResponse]: A tuple containing the response objects for table creation and schema setting.

            Raises:
                XataServerError: If the table creation or schema setting fails.
            """
            client = self._call_client(**self.client_kwargs)

            response1 = client.table().create(table_name, **kwargs)

            if not response1.is_success():
                raise XataServerError(response1.status_code, response1.server_message())

            response2 = client.table().set_schema(table_name, schema, **kwargs)

            if not response2.is_success():
                raise XataServerError(response2.status_code, response2.server_message())

            return response1, response2

    def delete_table(self, table_name: str, **kwargs) -> ApiResponse:
            """
            Deletes a table from the Xata database.

            Args:
                table_name (str): The name of the table to be deleted.
                **kwargs: Additional keyword arguments to be passed to the Xata client.

            Returns:
                ApiResponse: The response from the Xata client.

            Raises:
                XataServerError: If the table deletion fails.
            """
            client = self._call_client(**self.client_kwargs)
            response = client.table().delete(table_name, **kwargs)

            if not response.is_success():
                raise XataServerError(response.status_code, response.server_message())

            return response

    def create_column(self, table_name: str, column_config: dict, **kwargs) -> ApiResponse:
        """
        Creates a new column in the specified table.

        Args:
            table_name (str): The name of the table.
            column_config (dict): The configuration of the column.
            **kwargs: Additional keyword arguments to pass to the underlying API.

        Returns:
            ApiResponse: The response from the API.

        Raises:
            XataServerError: If the API response indicates an error.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.table().add_column(table_name, column_config, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def delete_column(self, table_name: str, column_name: str, **kwargs) -> ApiResponse:
        """
        Deletes a column from the specified table.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column.
            **kwargs: Additional keyword arguments to pass to the underlying API.

        Returns:
            ApiResponse: The response from the API.

        Raises:
            XataServerError: If the API response indicates an error.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.table().delete_column(table_name, column_name, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def get_columns(self, table_name: str, **kwargs) -> ApiResponse:
        """
        Retrieves the columns of the specified table.

        Args:
            table_name (str): The name of the table.
            **kwargs: Additional keyword arguments to pass to the underlying API.

        Returns:
            ApiResponse: The response from the API.

        Raises:
            XataServerError: If the API response indicates an error.
        """

        client = self._call_client(**self.client_kwargs)
        response = client.table().get_columns(table_name, **kwargs)

        if not response.is_success():
            raise XataServerError(response.status_code, response.server_message())

        return response

    def bulk_processor(self,**kwargs) -> BulkProcessor:
            """
            Additional abstraction for bulk requests that process' requests in parallel
            :stability beta

            Args:
                **kwargs: Additional keyword arguments to be passed to the BulkProcessor constructor.

            Returns:
                BulkProcessor: The created BulkProcessor object.
            """
            return BulkProcessor(self._call_client(**self.client_kwargs),**kwargs)

    def bulk_transaction(self,**kwargs) -> Transaction:
            """
            Additional abstraction for bulk requests that process' requests in parallel
            :stability beta

            Args:
                **kwargs: Additional keyword arguments to be passed to the BulkTransaction constructor.

            Returns:
                BulkTransaction: The created BulkTransaction object.
            """
            return Transaction(self._call_client(**self.client_kwargs),**kwargs)

    def api_request(self,method:str,url:str,payload: dict=None,
                    data: bytes=None,headers: dict=None,
                    is_streaming: bool=False) -> ApiResponse:
        """
        The function `api_request` takes in a method and a url and returns the response from the API.

        :param method: The method parameter is a string that represents the HTTP method to use for the request. It can be
        one of the following values: GET, POST, PUT, PATCH, DELETE
        :type method: str

        :param url: The url parameter is a string that represents the URL of the API endpoint
        :type url: str

        :param payload: The payload parameter is a dictionary that contains the data to be sent with the request
        :type payload: dict

        :param data: The data parameter is a bytes object that represents the data to be sent with the request
        :type data: bytes

        :param headers: The headers parameter is a dictionary that contains the headers to be sent with the request
        :type headers: dict

        :param is_streaming: The is_streaming parameter is a boolean value that indicates whether the request should be
        :type is_streaming: bool

        :return: an ApiResponse object.
        """
        client = self._call_client(**self.client_kwargs)
        response = ApiRequest(client).request(method,url,headers=headers,payload=payload,data=data,is_streaming=is_streaming)

        if not response.is_success():
            raise XataServerError(response.status_code,response.server_message())

        return response
