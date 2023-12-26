from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Literal, Optional, Tuple, Union, types


from streamlit.connections import BaseConnection
from xata.client import XataClient
from xata.helpers import to_rfc339
from xata.api_response import ApiResponse
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
                raise Exception(response.status_code)
            return response

        response = self._client.data().query(f'{self.table_name}',full_query,**kwargs)
        if not response.is_success():
            raise Exception(response.status_code)
        return self._client.data().query(f'{self.table_name}',full_query,**kwargs)

    def get_record(self,record_id:str,**kwargs) -> ApiResponse:
        """
        The function `get_record` retrieves a record from a table using the provided record ID.

        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record you want
        to retrieve
        :type record_id: str
        :return: an ApiResponse object.
        """
        return self._client.records().get(f'{self.table_name}',record_id,**kwargs)

    def get_many_records(self,**kwargs) -> ApiResponse:
        """
        The function `get_many_records` queries a data table using the provided arguments and returns the response as an
        `ApiResponse` object.
        :return: an ApiResponse object.
        """
        return self._client.data().query(f'{self.table_name}',**kwargs)

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
            return self._client.records().insert_with_id(f'{self.table_name}',record_id,record,**kwargs)

        return self._client.records().insert(f'{self.table_name}',record,**kwargs)

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

    """

    def __init__(self,connection_name:Optional[str]='xata',**kwargs):
        """
        The above function is a constructor that initializes an object with an optional connection name parameter.

        :param connection_name: The connection_name parameter is an optional string that specifies the name of the
        connection. If no value is provided, it defaults to 'xata', defaults to xata
        :type connection_name: Optional[str] (optional)
        """
        super().__init__(connection_name,**kwargs)


    def _connect(self,api_key:Optional[str]=None,db_url:Optional[str]=None,table_names:Optional[list]=None,**kwargs) -> None:
        """
        The `_connect` function establishes a connection to a database using an API key and a database URL.

        :param api_key: The API key used to authenticate and authorize access to the Xata API.
        It is a string that represents a unique identifier for your account or application.
        If not provided, it will be retrieved from the `XATA_API_KEY` environment variable or the secrets manager.
        :type api_key: Optional[str]
        :param db_url: The URL of the database that you want to connect to.
        It is a required parameter and must be provided either as an argument to the `_connect` method or through the `XATA_DB_URL` environment variable or the secrets manager.
        :type db_url: Optional[str]
        :param table_names: A list of table names that you want to access.
        If provided, the `_connect` function will create a `XataTable` object for each table name and assign it to a corresponding attribute in the `XataClient` object.
        :type table_names: Optional[list]
        :raises ConnectionRefusedError: If the API key or the database URL cannot be found.
        """
        if api_key is None:
            if "XATA_API_KEY" in self._secrets:
                api_key = self._secrets["XATA_API_KEY"]
            elif "XATA_API_KEY" in os.environ:
                api_key = os.environ.get("XATA_API_KEY")
            else:
                raise ConnectionRefusedError("No API key found. Please set the XATA_API_KEY environment variable or add it to the secrets manager.")
        if db_url is None:
            if "XATA_DB_URL" in self._secrets:
                db_url = self._secrets["XATA_DB_URL"]
            elif "XATA_DB_URL" in os.environ:
                db_url = os.environ.get("XATA_DB_URL")

        if db_url is None:
        #
            self._client = XataClient(api_key=api_key,**kwargs)
        else:
            self._client = XataClient(api_key=api_key,db_url=db_url,**kwargs)

        if table_names is not None and db_url is not None:
            self._table_names = table_names
            for table_name in table_names:
                setattr(self,table_name,XataTable(self._client,table_name))

    def query(self,table_name:str,full_query:Optional[dict]=None,consistency:Optional[Literal['strong','eventual']]=None,**kwargs) -> ApiResponse:
        """
        The function `query` takes a table name, a full query dictionary, a consistency level, and additional keyword
        arguments, and returns an API response.

        For more information visit: https://xata.io/docs/sdk/get

        :param table_name: The name of the table you want to query from
        :type table_name: str
        :param full_query: The `full_query` parameter is a dictionary that contains the details of the query to be executed.
        :type full_query: Optional[dict]
        :param consistency: The `consistency` parameter is an optional parameter that specifies the consistency level for
        the query. It can have two possible values: "strong" or "eventual". If set to "strong", the query will return the
        most up-to-date data, but it may have a higher latency.
        :type consistency: Optional[Literal['strong','eventual']]
        :return: an ApiResponse.
        """

        if consistency is not None and full_query is not None:
            full_query['consistency'] = consistency
        if full_query is None:
            return self._client.data().query(f'{table_name}',**kwargs)

        response = self._client.data().query(f'{table_name}',full_query,**kwargs)
        if not response.is_success():
            raise Exception(response.status_code)

        return self._client.data().query(f'{table_name}',full_query,**kwargs)

    def get_record(self,table_name:str,record_id:str,**kwargs) -> ApiResponse:
        """
        The function `get_record` retrieves a record from a specified table using the provided record ID.

        :param table_name: The name of the table from which you want to retrieve the record
        :type table_name: str
        :param record_id: The `record_id` parameter is a string that represents the unique identifier of a specific record
        in a table
        :type record_id: str
        :return: an ApiResponse object.
        """
        return self.client.records().get(f'{table_name}',record_id,**kwargs)

    def get_many_records(self,table_name:str,**kwargs) -> ApiResponse:
        """
        The function `get_many_records` retrieves multiple records from a specified table using the provided arguments.

        :param table_name: The name of the table from which you want to retrieve records
        :type table_name: str
        :return: an ApiResponse object.
        """
        return self.client.data().query(f'{table_name}',**kwargs)

    def insert(self,table_name:str,record:dict,record_id:Optional[str]=None,**kwargs) -> ApiResponse:
        """
        The function inserts a record into a table with an optional record ID.
        For more information visit: https://xata.io/docs/sdk/insert

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
            return self.client.records().insert_with_id(f'{table_name}',record_id,record,**kwargs)

        return self.client.records().insert(f'{table_name}',record,**kwargs)

    def replace(self,table_name:str,record_id:str,record:dict,**kwargs) -> ApiResponse:
        """
        The function replaces a record in a table with a new record.
        For more information visit: https://xata.io/docs/sdk/update

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
        return self.client.records().upsert(f'{table_name}',record_id,record,**kwargs)

    def update(self,table_name:str,record_id:str,record:dict,**kwargs) -> ApiResponse:
        """
        The function updates a record in a specified table using the provided record ID and record data.
        For more information visit: https://xata.io/docs/sdk/update
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
        return self.client.records().update(f'{table_name}',record_id,record,**kwargs)

    def delete(self,table_name:str,record_id:str,**kwargs) -> ApiResponse:
        """
        The function deletes a record from a specified table using the provided record ID.
        For more information visit: https://xata.io/docs/sdk/delete

        :param table_name: The name of the table from which you want to delete a record
        :type table_name: str
        :param record_id: The `record_id` parameter is a string that represents the unique identifier of the record that you
        want to delete from the specified table
        :type record_id: str
        :return: an ApiResponse.
        """
        return self.client.records().delete(f'{table_name}',record_id,**kwargs)

    def search(self,search_query:dict,**kwargs) -> ApiResponse:
        """
        The function takes a search query and additional keyword arguments, and returns the result of searching for a branch
        using the query.
        For more information visit: https://xata.io/docs/sdk/search

        :param search_query: A dictionary containing the search query parameters. The specific keys and values in the
        dictionary will depend on the API you are using for the search
        :type search_query: dict
        :return: an ApiResponse object.
        """
        return self.client.data().search_branch(search_query,**kwargs)

    def search_on_table(self,table_name:str,search_query:dict,**kwargs) -> ApiResponse:
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
        return self.client.data().search_table(f'{table_name}',search_query,**kwargs)

    def vector_search(self,table_name:str,search_query:dict,**kwargs) -> ApiResponse:
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
        return self.client.data().vector_search(f'{table_name}',search_query,**kwargs)

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
        return self.client.data().aggregate(f'{table_name}',aggregate_query,**kwargs)

    def summarize(self,table_name:str,summarize_query:dict,**kwargs) -> ApiResponse:
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
        return self.client.data().summarize(f'{table_name}',summarize_query,**kwargs)

    def transaction(self,payload:dict,**kwargs) -> ApiResponse:
        """
        The function `transaction` takes a transaction query and optional keyword arguments, and returns an `ApiResponse`
        object.

        For more information visit: https://xata.io/docs/sdk/transaction

        :param transaction_query: The transaction_query parameter is a dictionary that contains the details of the
        transaction. It may include information such as the transaction type, amount, date, and any other relevant details
        :type transaction_query: dict
        :return: an ApiResponse object.
        """
        return self.client.records().transaction(payload,**kwargs)

    def sql_query(self,query:str,**kwargs) -> ApiResponse:
        """
        The function `sql_query` takes a SQL query string and optional keyword arguments, and returns an `ApiResponse`
        object.

        :param query: The `query` parameter is a string that represents the SQL query you want to execute
        :type query: str
        :return: an ApiResponse object.
        """
        return self.client.sql().query(query,**kwargs)

    def askai(self,reference_table:str,question:str, rules: Optional[list]=[], options: Optional[dict]={},**kwargs)->ApiResponse:
        """
        The function `askAI` sends a question to an AI model using a reference table, rules, and options.

        :param reference_table: The reference_table parameter is a string that specifies the table or dataset from which the
        AI should retrieve information to answer the question
        :type reference_table: str
        :param question: The "question" parameter is a string that represents the question you want to ask the AI
        :type question: str
        :param rules: The `rules` parameter is an optional list that contains any additional rules or constraints that you
        want to apply to the question. These rules can be used to filter or manipulate the data before returning the answer
        :type rules: Optional[list]
        :param options: The "options" parameter is a dictionary that allows you to provide additional options for the AI to
        consider when answering the question. These options can include things like fuzziness, searchType, boosters, and any other
        relevant information that can help the AI provide a more accurate response
        :type options: Optional[dict]
        """
        self.client.data().ask(reference_table,question,rules,options,**kwargs)

    def askai_follow_up(self,reference_table:str,question:str,chatsessionid: str,**kwargs)->ApiResponse:
        return self.client.data().ask_follow_up(reference_table,chatsessionid,question,**kwargs)


    def __call__(self) -> XataClient:
        """
        The function returns the XataClient object.
        :return: The method is returning an instance of the XataClient class.
        """
        return self.client
