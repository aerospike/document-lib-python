from aerospike import Client
from jsonpath_ng import parse

import json

from typing import Any

class DocumentClient:
    def __init__(self, client: Client):
        self.client = client

    """
    Get an object from a JSON document using JSON path

    :param tuple key: the key of the record
    :param str binName: the name of the bin containing the JSON document
    :param str jsonPath: JSON path to retrieve the object
    :param dict readPolicy: the read policy for get() operation

    :return: :py:obj:`Any`
    :raises: :exc:`KeyNotFound`
    """
    def get(self, key: tuple, binName: str, jsonPath: str, readPolicy: dict = None) -> Any:
        # Get record bin containing JSON document
        _, _, bins = self.client.select(key, [binName], readPolicy)
        
        # Error: bin not found
        # select() will return a dictionary omitting the bin if it doesn't exist
        if binName not in bins:
            raise KeyError(f"Bin with name {binName} not found")
        
        # Parse document string into Python object
        document = bins[binName]
        jsonObj = json.loads(document)

        # Parse the object with JSON path
        expression = parse(jsonPath)
        results = [match.value for match in expression.find(jsonObj)]

        # Either return as list of multiple results or a single result
        if len(results) == 1:
            return results[0]
        else:
            return results

    """
    Put an object into a JSON document using JSON path

    :param tuple key: the key of the record
    :param str binName: the name of the bin containing the JSON document
    :param str jsonPath: JSON path location to store the object
    :param dict writePolicy: the write policy for put() operation
    """
    def put(self, key: tuple, binName: str, jsonPath: str, obj: Any, writePolicy: dict = None):
        pass

    """
    Append an object to a list in a JSON document using JSON path

    :param tuple key: the key of the record
    :param str binName: the name of the bin containing the JSON document
    :param str jsonPath: JSON path ending with a list
    :param dict writePolicy: the write policy for operate() operation
    """
    def append(self, key: tuple, binName: str, jsonPath: str, obj, writePolicy: dict = None):
        pass

    """
    Delete an object in a JSON document using JSON path

    :param tuple key: the key of the record
    :param str binName: the name of the bin containing the JSON document
    :param str jsonPath: JSON path of object to delete
    :param dict writePolicy: the write policy for operate() operation
    """
    def delete(self, key: tuple, binName: str, jsonPath: str):
        pass
